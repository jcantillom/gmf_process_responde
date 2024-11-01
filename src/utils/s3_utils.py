from botocore.exceptions import ClientError
from src.aws.clients import AWSClients


def check_file_exists_in_s3(bucket_name: str, file_key: str) -> bool:
    """
    Verifica si un archivo existe en el bucket de S3.
    """
    s3 = AWSClients.get_s3_client()
    try:
        s3.head_object(Bucket=bucket_name, Key=file_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        raise
