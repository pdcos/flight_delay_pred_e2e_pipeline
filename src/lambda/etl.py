import os 
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.dataset as ds
from dotenv import load_dotenv

from utils.s3_interface import S3Interface
from utils.glue_catalog_interface import GlueCatalogInterface

from utils.schemas import vra_raw_schema, vra_columns_name_remap

load_dotenv()

# Global Constants
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
BASE_URL = "https://siros.anac.gov.br/siros/registros/diversos/vra"
S3_BUCKET_NAME = "anac-flight-data"
RAW_DATA_FOLDER = "vra-raw"
GLUE_TABLE_NAME = RAW_DATA_FOLDER.replace("-", "_")
GLUE_CRAWLER_ROLE = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/iam_for_lambda"
COLUMN_NAME_MAP = vra_columns_name_remap
SCHEMA = vra_raw_schema

class FlightDataETL:
    def __init__(self):
        self.s3_interface = S3Interface()
        self.glue_interface = GlueCatalogInterface()
        self.raw_data_folder = RAW_DATA_FOLDER
        self.base_url = BASE_URL
        self.s3_path = f"s3://{S3_BUCKET_NAME}/{RAW_DATA_FOLDER}/"
        self.database_name = S3_BUCKET_NAME
        self.glue_table_name = GLUE_TABLE_NAME
        self.crawler_role = GLUE_CRAWLER_ROLE
        self.schema = SCHEMA
        self.dataframe = None
        self.year = None
        self.month = None

    def run(self, year: int, month: int) -> None:
        self.extract_data(year, month)
        self.transform_data()
        self.load_to_s3_and_register_glue()

    def extract_data(self, year: int, month: int) -> None:
        self.year = year
        self.month = month
        url = f"{self.base_url}/{year}/VRA_{year}_{month:02d}.csv"
        print(f"Downloading flight data from: {url}")

        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

        self.dataframe = pd.read_csv(url, sep=";", encoding="UTF-8")

    def transform_data(self) -> None:
        if self.dataframe is None:
            raise ValueError("No data available for transformation")

        self.dataframe.rename(columns=COLUMN_NAME_MAP, inplace=True)
        self.dataframe["reference"] = pd.to_datetime(self.dataframe["reference"], format="%Y-%m-%d")
        self.dataframe["year"] = self.dataframe["reference"].dt.year
        self.dataframe["month"] = self.dataframe["reference"].dt.month

    def load_to_s3_and_register_glue(self) -> None:
        if self.dataframe is None:
            raise ValueError("No data to load")

        self._save_parquet_to_s3()
        self._ensure_glue_table()

    def _save_parquet_to_s3(self) -> None:
        self.s3_interface.create_bucket(S3_BUCKET_NAME)

        arrow_table = pa.Table.from_pandas(self.dataframe, preserve_index=False)

        ds.write_dataset(
            arrow_table,
            self.s3_path,
            format="parquet",
            #partitioning=["year", "month"],
            partitioning=ds.partitioning(
                schema=pa.schema([pa.field("year", pa.int32()), pa.field("month", pa.int32())]),
                flavor="hive"
            ),
            existing_data_behavior="overwrite_or_ignore"
        )
        print(f"Data written to {self.s3_path}")

    def _ensure_glue_table(self) -> None:
        self.glue_interface.create_database(self.database_name)

        if not self.glue_interface.check_table_exists(self.database_name, self.glue_table_name):
            print(f"Glue table {self.glue_table_name} not found. Creating table...")

            table_input = self.glue_interface.generate_table_input(
                table_name=self.glue_table_name,
                columns_dict=self.schema,
                partition_keys=["year", "month"],
                s3_location=self.s3_path
            )

            self.glue_interface.create_table(
                database_name=self.database_name,
                table_input=table_input,
            )

            print(f"Glue table {self.glue_table_name} created.")
        else:
            print(f"Glue table {self.glue_table_name} already exists.")
        
        # Add partitions to the Glue table
        partitions = [{"year": self.year, "month": self.month}]
        self.glue_interface.add_partitions(
            database_name=self.database_name,
            table_name=self.glue_table_name,
            partitions=partitions,
            columns={k: v for k, v in self.schema.items() if k not in ["year", "month"]},
            s3_prefix=self.s3_path
        )
        print(f"Partitions {partitions} added to Glue table {self.glue_table_name}.")


class Scheduler:
    def __init__(self, 
                 etl: FlightDataETL):

        self.etl = etl
        self.glue_interface = GlueCatalogInterface()
        self.database_name = etl.database_name
        self.table_name = etl.glue_table_name

        self.coldstart_date = {"year": 2001, "month": 1}

    def check_table_exists(self):
        if not self.glue_interface.check_table_exists(self.database_name, self.table_name):
            print(f"Table {self.table_name} does not exist in database {self.database_name}.")
            return False
        return True
    
    def get_latest_partition(self):
        latest_partition = self.glue_interface.get_latest_partition(self.database_name, self.table_name)
        if latest_partition:
            print(f"Latest partition: {latest_partition}")
            return latest_partition
        else:
            print("No partitions found.")
            return None
    
    def get_next_execution_date(self):
        latest_partition = self.get_latest_partition()
        if latest_partition:
            year = latest_partition['year']
            month = latest_partition['month']
            if month == 12:
                return year + 1, 1
            else:
                return year, month + 1
        else:
            return None, None
        
    def run(self):


        month, year = None, None
        
        table_exists = self.check_table_exists()

        if not table_exists:
            print(f"Table {self.table_name} does not exist. Running ETL from coldstart date.")
            year, month = self.coldstart_date['year'], self.coldstart_date['month']
        else:
            year, month = self.get_next_execution_date()
            print(f"Next execution date: {year}-{month:02d}")

        if year is not None and month is not None:
            print(f"Running ETL for year {year} and month {month}.")
            self.etl.run(year=year, month=month)

        return
    
    def full_load(self, init_date, end_date):
        """
        Full load from init_date to end_date
        """
        start_year, start_month = init_date
        end_year, end_month = end_date

        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                if year == start_year and month < start_month:
                    continue
                if year == end_year and month > end_month:
                    break
                self.etl.run(year=year, month=month)









if __name__ == "__main__":

    etl = FlightDataETL()
    scheduler = Scheduler(etl=etl)
    #scheduler.run()
    scheduler.full_load(init_date=(2022, 8), end_date=(2023, 2))    
    # try:
    #     etl.run(year=2023, month=1)
    # except Exception as error:
    #     print(f"An error occurred during the ETL process: {error}")
