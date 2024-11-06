import json
import os
import re

from src.config.config import env


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


def extract_date_from_filename(filename: str) -> str:
    """
    Extrae la fecha en formato 'YYYYMMDD' de un nombre de archivo específico.

    Args:
        filename (str): El nombre del archivo del cual extraer la fecha.

    Returns:
        str: La fecha extraída en formato 'YYYYMMDD', o una cadena vacía si no se encuentra.
    """
    # Define la expresión regular para extraer la fecha
    prefix = env.CONST_PRE_SPECIAL_FILE
    pattern = rf'{prefix}_TUTGMF\d{{8}}(\d{{8}})-\d{{4}}\.zip$'
    match = re.search(pattern, filename)

    if match:
        # Devuelve la fecha extraída
        return match.group(1)

    # Si no se encuentra coincidencia, devuelve una cadena vacía
    return ''


def create_file_id(filename: str) -> int:
    """
    Crea un identificador único para un archivo.

    Returns:
        str: Un identificador único para un archivo.
    """
    date = extract_date_from_filename(filename)

    if date:
        componente1 = "01"
        componente2 = "05"
        componente3 = "0001"

        return int(f"{date}{componente1}{componente2}{componente3}")


def create_filename_without_prefix_and_extension(filename: str) -> str:
    """
    Crea un nombre de archivo sin el prefijo y la extensión.
    """
    # Remueve la extensión .zip
    filename_without_ext = filename.rsplit(".", 1)[0]

    # Remueve el prefijo exacto, basado en el valor de la variable de entorno
    prefix_special = env.CONST_PRE_SPECIAL_FILE
    prefix_general = env.CONST_PRE_GENERAL_FILE

    if filename_without_ext.startswith(prefix_special):
        filename_without_prefix = filename_without_ext[len(prefix_special):].lstrip('_')
    elif filename_without_ext.startswith(prefix_general):
        filename_without_prefix = filename_without_ext[len(prefix_general):].lstrip('_')
    else:
        filename_without_prefix = filename_without_ext

    return filename_without_prefix
