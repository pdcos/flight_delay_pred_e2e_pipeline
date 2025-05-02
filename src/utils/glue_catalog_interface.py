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

    def create_table(self, database_name: str, table_name: str, table_input: dict) -> None:
        """
        Create a Glue table in a specified database.
        """
        if not self.check_table_exists(database_name, table_name):
            self.glue.create_table(DatabaseName=database_name, TableInput=table_input)
            print(f"Table '{table_name}' created in database '{database_name}'.")
        else:
            print(f"Table '{table_name}' already exists in database '{database_name}'.")

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