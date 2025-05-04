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

    def create_colname_typing_dict(self) -> dict:
        """
        Create a dictionary for column name typing.
        """
        return {
            "icao_airline_code": "string",
            "airline_name": "string",
            "flight_number": "string",
            "di_code": "string",
            "line_type_code": "string",
            "aircraft_model": "string",
            "seat_count": "int",
            "origin_airport_icao": "string",
            "origin_airport_name": "string",
            "scheduled_departure": "timestamp",
            "actual_departure": "timestamp",
            "destination_airport_icao": "string",
            "destination_airport_name": "string",
            "scheduled_arrival": "timestamp",
            "actual_arrival": "timestamp",
            "flight_status": "string",
            "justification": "string",
            "reference": "timestamp",
            "departure_status": "string",
            "arrival_status": "string",
            "month": "int",
            "year": "int",
        }

class Scheduler:
    def __init__(self, etl: FlightDataETL):
        self.etl = etl

    def schedule(self, year: int, month: int) -> None:
        self.etl.run(year, month)

if __name__ == "__main__":
    etl = FlightDataETL()
    try:
        etl.run(year=2023, month=3)
    except Exception as error:
        print(f"An error occurred during the ETL process: {error}")
