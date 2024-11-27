import os
import sys
from pydantic_settings import BaseSettings
from pydantic import ValidationError
from src.utils.logger_utils import get_logger

logger = get_logger(__name__)


class EnvironmentSettings(BaseSettings):
    """
    Clase para definir las variables de entorno del proyecto.
    """
    APP_ENV: str = "test"
    SECRETS_DB: str = "default_secret_db"
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "test_db"
    DEBUG_MODE: bool = True
    SQS_URL_PRO_RESPONSE_TO_PROCESS: str = ""
    SQS_URL_EMAILS: str = ""
    PARAMETER_STORE_FILE_CONFIG: str = "/gmf/process-responses/general-config"
    SPECIAL_START_NAME: str = ""
    SPECIAL_END_NAME: str = ""
    GENERAL_START_NAME: str = ""
    CONST_PRE_SPECIAL_FILE: str = ""
    CONST_PRE_GENERAL_FILE: str = ""
    CONST_ID_PLANTILLA_EMAIL: str = ""
    CONST_COD_ERROR_EMAIL: str = ""
    DIR_RECEPTION_FILES: str = ""
    DIR_PROCESSED_FILES: str = ""
    DIR_REJECTED_FILES: str = ""
    DIR_PROCESSING_FILES: str = ""
    VALID_STATES_FILES: str = ""
    CONST_ESTADO_PROCESSED: str = ""
    CONST_ESTADO_LOAD_RTA_PROCESSING: str = ""
    CONST_ESTADO_INICIADO: str = ""
    CONST_ESTADO_REJECTED: str = ""
    CONST_ESTADO_PROCESAMIENTO_RECHAZADO: str = ""
    CONST_ID_PLANTILLA_CORREO_ERROR_DECOMPRESION: str = ""
    CONST_COD_ERROR_STRUCTURE_NAME_FILE: str = ""
    CONST_COD_ERROR_INVALID_FILE_SUFFIX: str = ""
    CONST_COD_ERROR_UNEXPECTED_FILE_COUNT: str = ""
    CONST_COD_ERROR_TECHNICAL_UNZIP: str = ""
    CONST_COD_ERROR_STATE_FILE: str = ""
    CONST_COD_ERROR_NOT_EXISTS_FILE: str = ""
    CONST_COD_ERROR_CORRUPTED_FILE: str = ""
    SUFFIX_RESPONSE_DEBITO: str = ""
    SUFFIX_RESPONSE_REINTEGROS: str = ""
    SUFFIX_RESPONSE_ESPECIALES: str = ""
    SQS_URL_PRO_RESPONSE_TO_UPLOAD: str = ""
    CONST_ESTADO_INIT_PENDING: str = ""
    S3_BUCKET_NAME: str = ""
    SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE: str = ""
    CONST_ESTADO_SEND: str = ""
    CONST_TIPO_ARCHIVO_ESPECIAL: str = "05"
    CONST_PLATAFORMA_ORIGEN: str = ""
    CONST_TIPO_ARCHIVO_GENERAL: str = ""
    CONST_TIPO_ARCHIVO_GENERAL_REINTEGROS: str = ""
    PARAMETER_STORE_TRANSVERSAL: str = ""
    CONST_COD_ERROR_TECHNICAL: str = ""

    class Config:
        env_file = ".env"


try:
    env: EnvironmentSettings = EnvironmentSettings()
except ValidationError as e:
    for error in e.errors():
        field = error.get("loc", ["Campo desconocido"])[0]
        error_message = error.get("msg", "Error de validación no especificado")
        logger.error(f"Error de validación en el campo '{field}': {error_message}")
    logger.error("Errores encontrados en la validación de las variables de entorno, verifique el archivo '.env'")
    sys.exit(1)
