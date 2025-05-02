import os 
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.dataset as ds
from dotenv import load_dotenv

from utils.s3_interface import S3Interface
from utils.glue_catalog_interface import GlueCatalogInterface

load_dotenv()

# Global Constants
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID")
BASE_URL = "https://siros.anac.gov.br/siros/registros/diversos/vra"
S3_BUCKET_NAME = "anac-flight-data"
RAW_DATA_FOLDER = "vra-raw"
GLUE_TABLE_NAME = RAW_DATA_FOLDER.replace("-", "_")
GLUE_CRAWLER_ROLE = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/iam_for_lambda"
COLUMN_NAME_MAP = {
    'Sigla ICAO Empresa Aérea': 'icao_airline_code',
    'Empresa Aérea': 'airline_name',
    'Número Voo': 'flight_number',
    'Código DI': 'di_code',
    'Código Tipo Linha': 'line_type_code',
    'Modelo Equipamento': 'aircraft_model',
    'Número de Assentos': 'seat_count',
    'Sigla ICAO Aeroporto Origem': 'origin_airport_icao',
    'Descrição Aeroporto Origem': 'origin_airport_name',
    'Partida Prevista': 'scheduled_departure',
    'Partida Real': 'actual_departure',
    'Sigla ICAO Aeroporto Destino': 'destination_airport_icao',
    'Descrição Aeroporto Destino': 'destination_airport_name',
    'Chegada Prevista': 'scheduled_arrival',
    'Chegada Real': 'actual_arrival',
    'Situação Voo': 'flight_status',
    'Justificativa': 'justification',
    'Referência': 'reference',
    'Situação Partida': 'departure_status',
    'Situação Chegada': 'arrival_status',
}

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
            partitioning=["year", "month"],
            existing_data_behavior="overwrite_or_ignore"
        )
        print(f"Data written to {self.s3_path}")

    def _ensure_glue_table(self) -> None:
        self.glue_interface.create_database(self.database_name)

        if not self.glue_interface.check_table_exists(self.database_name, self.glue_table_name):
            print(f"Glue table {self.glue_table_name} not found. Creating crawler...")

            self.glue_interface.delete_crawler(self.glue_table_name)
            self.glue_interface.create_crawler(
                crawler_name=self.glue_table_name,
                role=self.crawler_role,
                database_name=self.database_name,
                table_name=self.raw_data_folder,
            )
            self.glue_interface.start_crawler(self.glue_table_name)
            self.glue_interface.wait_for_crawler(self.glue_table_name)
            print(f"Crawler completed for Glue table {self.glue_table_name}.")
        else:
            print(f"Glue table {self.glue_table_name} already exists.")

if __name__ == "__main__":
    etl = FlightDataETL()
    try:
        etl.run(year=2023, month=2)
    except Exception as error:
        print(f"An error occurred during the ETL process: {error}")
