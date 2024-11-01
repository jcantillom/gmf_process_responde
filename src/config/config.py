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
