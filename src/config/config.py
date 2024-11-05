import os
import sys
from pydantic_settings import BaseSettings
from pydantic import ValidationError
from src.logs.logger import get_logger
from src.utils.singleton import SingletonMeta

logger = get_logger(__name__)


class EnvironmentSettings(BaseSettings):
    """
    Clase para definir las variables de entorno del proyecto.
    """
    APP_ENV: str
    SECRETS_DB: str
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DEBUG_MODE: bool
    SQS_URL_PRO_RESPONSE_TO_PROCESS: str
    SQS_URL_EMAILS: str
    PARAMETER_STORE_FILE_CONFIG: str
    SPECIAL_START_NAME: str
    SPECIAL_END_NAME: str
    GENERAL_START_NAME: str
    CONST_PRE_SPECIAL_FILE: str
    CONST_PRE_GENERAL_FILE: str
    CONST_ID_PLANTILLA_EMAIL: str
    CONST_COD_ERROR_EMAIL: str
    DIR_RECEPTION_FILES: str
    DIR_PROCESSED_FILES: str
    DIR_REJECTED_FILES: str
    DIR_PROCESSING_FILES: str
    VALID_STATES_FILES: str
    CONST_ESTADO_PROCESSED: str
    CONST_ESTADO_LOAD_RTA_PROCESSING: str
    CONST_ESTADO_INICIADO: str
    CONST_ESTADO_REJECTED: str
    CONST_ESTADO_PROCESAMIENTO_RECHAZADO: str
    CONST_ID_PLANTILLA_CORREO_ERROR_DECOMPRESION: str
    CONST_COD_ERROR_DECOMPRESION: str
    SUFFIX_RESPONSE_DEBITO: str
    SUFFIX_RESPONSE_REINTEGROS: str
    SUFFIX_RESPONSE_ESPECIALES: str
    SQS_URL_PRO_RESPONSE_TO_UPLOAD: str
    CONST_ESTADO_INIT_PENDING: str
    S3_BUCKET_NAME: str
    SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE: str

    class Config:
        env_file = os.path.join(os.getcwd(), ".env")
        env_file_encoding = "utf-8"


# Cargar y validar la configuración con manejo de errores
try:
    env: EnvironmentSettings = EnvironmentSettings()
except ValidationError as e:
    for error in e.errors():
        field = error.get("loc", ["Campo desconocido"])[0]
        error_message = error.get("msg", "Error de validación no especificado")
        logger.error(f"Error de validación en el campo '{field}': {error_message}")
    logger.error("Errores encontrados en la validación de las variables de entorno, verifique el archivo '.env'")
    sys.exit(1)
