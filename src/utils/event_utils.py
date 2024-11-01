import json
import os


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
