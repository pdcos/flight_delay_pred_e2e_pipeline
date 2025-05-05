import boto3
import time
import botocore.exceptions

class GlueCatalogInterface:
    def __init__(self, region_name: str = "us-east-1"):
        self.region_name = region_name
        self.glue = boto3.client('glue', region_name=self.region_name)

    def check_database_exists(self, database_name: str) -> bool:
        """
        Check if a Glue database exists.
        """
        try:
            self.glue.get_database(Name=database_name)
            return True
        except self.glue.exceptions.EntityNotFoundException:
            return False
        
    def check_table_exists(self, database_name: str, table_name: str) -> bool:
        """
        Check if a Glue table exists in a specified database.
        """
        try:
            self.glue.get_table(DatabaseName=database_name, Name=table_name)
            return True
        except self.glue.exceptions.EntityNotFoundException:
            return False
        
    def create_database(self, database_name: str) -> None:
        """
        Create a Glue database if it does not exist.
        """
        if not self.check_database_exists(database_name):
            self.glue.create_database(DatabaseInput={'Name': database_name})
            print(f"Database '{database_name}' created.")
        else:
            print(f"Database '{database_name}' already exists.")

    def delete_database(self, database_name: str) -> None:
        """
        Delete a Glue database if it exists.
        """
        if self.check_database_exists(database_name):
            self.glue.delete_database(Name=database_name)
            print(f"Database '{database_name}' deleted.")
        else:
            print(f"Database '{database_name}' does not exist.")

    def create_crawler(self, crawler_name: str, role: str, database_name: str, table_name: str) -> None:
        """
        Create a Glue crawler.
        """
        try:
            self.glue.create_crawler(
                Name=crawler_name,
                Role=role,
                DatabaseName=database_name,
                Targets={'S3Targets': [{'Path': f's3://{database_name}/{table_name}'}]},
                #TablePrefix="_teste"
            )
            print(f"Crawler '{crawler_name}' created.")
        except Exception as e:
            print(f"Error creating crawler: {e}")

    def start_crawler(self, crawler_name: str) -> None:
        """
        Start a Glue crawler.
        """
        try:
            self.glue.start_crawler(Name=crawler_name)
            print(f"Crawler '{crawler_name}' started.")
        except Exception as e:
            print(f"Error starting crawler: {e}")

    def check_crawler_exists(self, crawler_name: str) -> bool:
        """
        Check if a Glue crawler exists.
        """
        try:
            self.glue.get_crawler(Name=crawler_name)
            return True
        except self.glue.exceptions.EntityNotFoundException:
            return False
        

    def wait_for_crawler(self, crawler_name: str, wait_interval: int = 10, timeout: int = 300) -> None:
        """
        Waits for a Glue crawler to finish its current state (either running or stopping).
        
        :param crawler_name: Name of the crawler to wait for
        :param wait_interval: Time in seconds between status checks
        :param timeout: Max time in seconds to wait for the crawler to complete
        """
        elapsed = 0
        while elapsed < timeout:
            try:
                response = self.glue.get_crawler(Name=crawler_name)
                state = response['Crawler']['State']
                if state == 'READY':
                    print(f"Crawler '{crawler_name}' is ready.")
                    return
                else:
                    print(f"Crawler state: {state}. Waiting...")
                    time.sleep(wait_interval)
                    elapsed += wait_interval
            except botocore.exceptions.ClientError as e:
                print(f"Error getting crawler status: {e}")
                break

        raise TimeoutError(f"Crawler '{crawler_name}' did not become ready within {timeout} seconds.")
        
    def delete_crawler(self, crawler_name: str) -> None:
        """
        Delete a Glue crawler if it exists.
        """
        if self.check_crawler_exists(crawler_name):
            self.glue.delete_crawler(Name=crawler_name)
            print(f"Crawler '{crawler_name}' deleted.")
        else:
            print(f"Crawler '{crawler_name}' does not exist.")

    def generate_table_input(self, table_name, columns_dict, s3_location, partition_keys=["year", "month"]):
        """
        Gera o dicionário TableInput necessário para criar a tabela no Glue.
        """
        def format_columns(col_dict):
            return [{"Name": k, "Type": v} for k, v in col_dict.items()]

        table_input = {
            "Name": table_name,
            "StorageDescriptor": {
                "Columns": format_columns({k: v for k, v in columns_dict.items() if k not in partition_keys}),
                "Location": s3_location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {}
                }
            },
            "PartitionKeys": format_columns({k: columns_dict[k] for k in partition_keys}),
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"EXTERNAL": "TRUE"}
        }
        return table_input

    def create_table(self, database_name, table_input):
        """
        Cria a tabela no Glue Data Catalog.
        """
        try:
            self.glue.create_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
            print(f"Tabela '{table_input['Name']}' criada com sucesso.")
        except self.glue.exceptions.AlreadyExistsException:
            print(f"Tabela '{table_input['Name']}' já existe.")

    def add_partitions(self, database_name, table_name, partitions, columns, s3_prefix):
        """
        Adiciona partições manualmente à tabela no Glue.
        """
        partition_input_list = []
        for p in partitions:
            values = [str(p["year"]), str(p["month"])]
            location = f'{s3_prefix}year={p["year"]}/month={p["month"]}/'

            partition_input = {
                "Values": values,
                "StorageDescriptor": {
                    "Columns": [{"Name": k, "Type": v} for k, v in columns.items()],
                    "Location": location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {}
                    }
                }
            }
            partition_input_list.append(partition_input)

        response = self.glue.batch_create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInputList=partition_input_list
        )

        print("Partitions added successfully.")
        return response
    
    def get_latest_partition(self, database_name, table_name):
        client = boto3.client('glue')

        paginator = client.get_paginator('get_partitions')
        response_iterator = paginator.paginate(
            DatabaseName=database_name,
            TableName=table_name
        )

        partitions = []
        for page in response_iterator:
            for partition in page['Partitions']:
                values = partition['Values']  # Ex: ['2023', '02']
                if len(values) >= 2:
                    try:
                        year = int(values[0])
                        month = int(values[1])
                        partitions.append((year, month))
                    except ValueError:
                        continue  # ignora partições mal formatadas

        if not partitions:
            return None

        latest = max(partitions)
        return {'year': latest[0], 'month': latest[1]}



if __name__ == "__main__":
    glue_interface = GlueCatalogInterface()
    database_name = "anac-flight-data"
    
    if glue_interface.check_database_exists(database_name):
        print(f"Database '{database_name}' exists.")
    else:
        print(f"Database '{database_name}' does not exist.")

    table_name = "vra_raw"
    if glue_interface.check_table_exists(database_name, table_name):
        print(f"Table '{table_name}' exists in database '{database_name}'.")

    from schemas import vra_raw_schema

    table_name = "var_raw_test_2"
    s3_location = "s3://anac-flight-data/vra-raw/"
    table_input = glue_interface.generate_table_input(table_name, vra_raw_schema, s3_location=s3_location)
    #glue_interface.create_table(database_name, table_input)

    partitions = [{"year": 2023, "month": 3}, {"year": 2023, "month": 2}]

    glue_interface.add_partitions(
        database_name=database_name,
        table_name=table_name,
        partitions=partitions,
        columns={k: v for k, v in vra_raw_schema.items() if k not in ["year", "month"]},
        s3_prefix=s3_location
    )

    partitions = glue_interface.get_latest_partition(database_name, table_name)
    print(f"Latest partition: {partitions}")    
