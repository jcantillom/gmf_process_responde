# src/config/config.py
import os
import sys

from pydantic_settings import BaseSettings
from pydantic import ValidationError
from src.logs.logger import logger  # Asegúrate de que el logger esté configurado


class EnvironmentSettings(BaseSettings):
    """
    Clase para definir las variables de entorno del proyecto.
    """
    APP_ENV: str
    SECRETS_DB: str
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str

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
        logger.error(f"Error en la configuración de '{field}': {error_message}")
    logger.error(
        "Errores encontrados en las variables de entorno. Verifica las variables de entorno y vuelve a intentarlo.")
    sys.exit(1)
