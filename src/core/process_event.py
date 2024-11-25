import json
import os
import re
from src.config.config import env, logger
from src.core.validator import ArchivoValidator


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


def create_file_id(filename: str):
    """
    Crea un identificador único para un archivo.

    Returns:
        str: Un identificador único para un archivo.
    """
    archivo_validator = ArchivoValidator()
    date = extract_date_from_filename(filename)

    if not date:
        logger.error("Fecha no encontrada en el nombre del archivo",
                     extra={"filename": filename})
        return

    componente1 = env.CONST_PLATAFORMA_ORIGEN
    componente2 = env.CONST_TIPO_ARCHIVO_ESPECIAL
    componente3 = archivo_validator.special_end.zfill(4)

    return int(f"{date}{componente1}{componente2}{componente3}")


def build_acg_name_if_general_file(acg_nombre_archivo: str) -> str:
    """
    Esta Function se encarga de quitar el prefijo env.CONST_PRE_GENERAL_FILE,
    al nombre del archivo.
    """
    acg_nombre_archivo = acg_nombre_archivo.replace(env.CONST_PRE_GENERAL_FILE, "")
    return acg_nombre_archivo.lstrip("_")


def extract_consecutivo_plataforma_origen(filename: str) -> str:
    """
    Extrae el consecutivo de la plataforma de origen del filename.
    Ejemplo: RE_ESP_TUTGMF0001003920241002-0001.zip, me quedo con 0001
    """
    # Define la expresión regular para extraer el consecutivo de la plataforma de origen
    prefix = env.CONST_PRE_SPECIAL_FILE
    pattern = rf'{prefix}_TUTGMF\d{{8}}\d{{8}}-(\d{{4}})\.zip$'
    match = re.search(pattern, filename)

    return match.group(1) if match else ''
