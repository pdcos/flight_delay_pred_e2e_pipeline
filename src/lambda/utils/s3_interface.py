import boto3

class S3Interface():
    def __init__(self, region_name: str = "us-east-1"):
        self.region_name = region_name
        self.s3 = boto3.client('s3', region_name=self.region_name)

    def check_bucket_exists(self, bucket_name: str = None):
        """
        Check if an S3 bucket exists
        """
        try:
            response = self.s3.list_buckets()
            buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
            return bucket_name in buckets
        except Exception as e:
            print(f"Error checking bucket existence: {e}")
            return False

    def create_bucket(self, bucket_name: str = None):
        """
        Create an S3 bucket if it does not already exist
        """
        try:
            bucket_exists = self.check_bucket_exists(bucket_name)
            if bucket_exists:
                print(f"Bucket {bucket_name} already exists")
            else:
                self.s3.create_bucket(Bucket=bucket_name)
                print(f"Bucket {bucket_name} created")
        except Exception as e:
            print(f"Error creating bucket: {e}")

    def delete_bucket(self, bucket_name: str = None):
        """
        Delete an S3 bucket if it exists
        """
        try:
            bucket_exists = self.check_bucket_exists(bucket_name)
            if not bucket_exists:
                print(f"Bucket {bucket_name} does not exist")
            else:
                self.s3.delete_bucket(Bucket=bucket_name)
                print(f"Bucket {bucket_name} deleted")
        except Exception as e:
            print(f"Error deleting bucket: {e}")

    def upload_file(self, file_path: str = None, bucket_name = None, object_name: str = None):
        """
        Upload a file to an S3 bucket
        """
        try:
            self.s3.upload_file(file_path, bucket_name, object_name)
            print(f"File {file_path} uploaded to {bucket_name}/{object_name}")
        except Exception as e:
            print(f"Error uploading file: {e}")

    def delete_file(self, bucket_name: str = None, object_name: str = None):
        """
        Delete a file from an S3 bucket
        """
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=object_name)
            print(f"File {object_name} deleted from {bucket_name}")
        except Exception as e:
            print(f"Error deleting file: {e}")

    def download_file():
        #TODO: Implement download file method
        return


if __name__ == "__main__":
    # Example usage
    bucket_name = "fazendo-algo"
    s3_interface = S3Interface()
    s3_interface.create_bucket(bucket_name=bucket_name)
    file_path = "Dockerfile"
    s3_interface.upload_file(file_path=file_path, bucket_name=bucket_name, object_name="Dockerfile")
    s3_interface.delete_file(bucket_name=bucket_name, object_name="Dockerfile")
    s3_interface.delete_bucket(bucket_name=bucket_name)