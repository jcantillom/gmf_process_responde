import json
import os

import boto3
from botocore.exceptions import ClientError
from src.logs import logger as log
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


def extract_filename_from_body(body: str) -> str:
    """
    Extrae solo el nombre del archivo del cuerpo del mensaje, eliminando el prefijo del directorio.
    """
    event_data = json.loads(body)
    file_key = event_data["Records"][0]["s3"]["object"]["key"]
    filename = os.path.basename(file_key)
    return filename


def extract_bucket_from_body(body: str) -> str:
    """
    Extrae el nombre del bucket del cuerpo del mensaje.
    """
    event_data = json.loads(body)
    bucket_name = event_data["Records"][0]["s3"]["bucket"]["name"]
    return bucket_name
