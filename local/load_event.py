import json
import os
from src.logs.logger import get_logger
from src.config.config import env

logger = get_logger(env.DEBUG_MODE)


def load_local_event():
    """
    Carga el archivo JSON de prueba desde el entorno local para desarrollo.

    :return: Datos del evento en formato JSON, o un diccionario vacío si falla.
    """
    file_path = "test_data/event.json"

    try:
        with open(file_path, 'r') as file:
            event_data = json.load(file)
            logger.debug("Archivo de prueba local cargado exitosamente: %s", file_path)
            return event_data
    except FileNotFoundError:
        logger.error("Archivo de prueba no encontrado en %s", file_path)
    except json.JSONDecodeError:
        logger.error("Error al parsear el archivo JSON: %s", file_path)

    return {}  # Retorna un evento vacío en caso de error
