import os
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.dataset as ds
from dotenv import load_dotenv

from utils.s3_interface import S3Interface
from utils.glue_catalog_interface import GlueCatalogInterface
from utils.schemas import vra_raw_schema, vra_columns_name_remap


class FlightDataETL:
    def __init__(self,
                 s3_bucket: str,
                 raw_data_folder: str,
                 base_url: str,
                 aws_account_id: str,
                 schema: dict,
                 column_name_map: dict):
        
        self.s3_interface = S3Interface()
        self.glue_interface = GlueCatalogInterface()

        self.s3_bucket = s3_bucket
        self.raw_data_folder = raw_data_folder
        self.base_url = base_url
        self.s3_path = f"s3://{s3_bucket}/{raw_data_folder}/"
        self.database_name = s3_bucket
        self.glue_table_name = raw_data_folder.replace("-", "_")
        self.crawler_role = f"arn:aws:iam::{aws_account_id}:role/iam_for_lambda"
        self.schema = schema
        self.column_name_map = column_name_map
        self.dataframe = None
        self.year = None
        self.month = None

    def run(self, year: int, month: int) -> None:
        try:
            self.extract_data(year, month)
            self.transform_data()
            self.load_to_s3_and_register_glue()
            print(f"ETL completed successfully for {year}-{month:02d}.")
        except FileNotFoundError as e:
            print(f"Data for {year}-{month:02d} is not available yet. Skipping. [{e}]")
        except Exception as e:
            print(f"ETL failed for {year}-{month:02d}: {e}")

    def extract_data(self, year: int, month: int) -> None:
        self.year = year
        self.month = month
        url = f"{self.base_url}/{year}/VRA_{year}_{month:02d}.csv"
        print(f"Attempting to download: {url}")

        response = requests.get(url)
        if response.status_code != 200:
            raise FileNotFoundError(f"HTTP {response.status_code} - Data not found")

        self.dataframe = pd.read_csv(url, sep=";", encoding="UTF-8")

    def transform_data(self) -> None:
        if self.dataframe is None:
            raise ValueError("Data not extracted")

        self.dataframe.rename(columns=self.column_name_map, inplace=True)
        self.dataframe["reference"] = pd.to_datetime(self.dataframe["reference"], format="%Y-%m-%d")
        self.dataframe["year"] = self.dataframe["reference"].dt.year
        self.dataframe["month"] = self.dataframe["reference"].dt.month

    def load_to_s3_and_register_glue(self) -> None:
        if self.dataframe is None:
            raise ValueError("No data to load")

        self._save_parquet_to_s3()
        self._ensure_glue_table()

    def _save_parquet_to_s3(self) -> None:
        self.s3_interface.create_bucket(self.s3_bucket)

        arrow_table = pa.Table.from_pandas(self.dataframe, preserve_index=False)

        ds.write_dataset(
            arrow_table,
            self.s3_path,
            format="parquet",
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
            print(f"Creating Glue table {self.glue_table_name}...")
            table_input = self.glue_interface.generate_table_input(
                table_name=self.glue_table_name,
                columns_dict=self.schema,
                partition_keys=["year", "month"],
                s3_location=self.s3_path
            )
            self.glue_interface.create_table(self.database_name, table_input)
            print(f"Glue table {self.glue_table_name} created.")
        else:
            print(f"Glue table {self.glue_table_name} already exists.")

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
    def __init__(self, etl: FlightDataETL, coldstart_date: dict):
        self.etl = etl
        self.glue_interface = GlueCatalogInterface()
        self.database_name = etl.database_name
        self.table_name = etl.glue_table_name
        self.coldstart_date = coldstart_date

    def run(self) -> None:
        if not self._table_exists():
            print("No Glue table found. Coldstart initiated.")
            year, month = self.coldstart_date['year'], self.coldstart_date['month']
        else:
            year, month = self._get_next_execution_date()

        if year is not None and month is not None:
            print(f"Executing ETL for {year}-{month:02d}...")
            self.etl.run(year, month)

    def full_load(self, init_date: tuple, end_date: tuple) -> None:
        start_year, start_month = init_date
        end_year, end_month = end_date

        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                if (year == start_year and month < start_month) or (year == end_year and month > end_month):
                    continue
                print(f"Executing ETL for {year}-{month:02d}...")
                self.etl.run(year=year, month=month)

    def _table_exists(self) -> bool:
        return self.glue_interface.check_table_exists(self.database_name, self.table_name)

    def _get_latest_partition(self):
        latest_partition = self.glue_interface.get_latest_partition(self.database_name, self.table_name)
        if latest_partition:
            print(f"Latest partition: {latest_partition}")
        else:
            print("No partitions found.")
        return latest_partition

    def _get_next_execution_date(self):
        latest_partition = self._get_latest_partition()
        if latest_partition:
            year = latest_partition['year']
            month = latest_partition['month']
            if month == 12:
                return year + 1, 1
            return year, month + 1
        return None, None


if __name__ == "__main__":
    load_dotenv()
    AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")

    etl = FlightDataETL(
        s3_bucket="anac-flight-data",
        raw_data_folder="vra-raw",
        base_url="https://siros.anac.gov.br/siros/registros/diversos/vra",
        aws_account_id=AWS_ACCOUNT_ID,
        schema=vra_raw_schema,
        column_name_map=vra_columns_name_remap
    )

    scheduler = Scheduler(etl=etl, coldstart_date={"year": 2001, "month": 1})

    scheduler.run()
    #scheduler.full_load(init_date=(2025, 8), end_date=(2026, 2))
