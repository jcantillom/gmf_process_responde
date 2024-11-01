# src/utils/log_utils.py

import logging
import os
import pytz
from datetime import datetime
from colorama import Fore, Style

# Configuración de colores para los niveles de log
LOG_COLORS = {
    "INFO": Fore.GREEN,
    "ERROR": Fore.RED,
    "WARNING": Fore.YELLOW,
    "DEBUG": Fore.BLUE
}


class CustomFormatter(logging.Formatter):
    """
    Formato de log personalizado que incluye fecha, hora, nivel, archivo, línea, y mensaje.
    """

    def format(self, record):
        # Obtener la hora actual en la zona horaria de Colombia
        colombia_tz = pytz.timezone("America/Bogota")
        record_time = datetime.now(colombia_tz).strftime("%Y-%m-%d %H:%M:%S")

        # Nivel de log en color
        level_color = LOG_COLORS.get(record.levelname, "")
        level_name = f"{level_color}[{record.levelname}]{Style.RESET_ALL}"

        # Obtener el nombre del archivo con el directorio relativo al proyecto
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        relative_path = os.path.relpath(record.pathname, start=project_root)
        file_path = f"[{relative_path}:{record.lineno}]"

        # Agregar el nombre del archivo del evento si existe
        event_filename = getattr(record, "event_filename", None)
        event_filename_str = f"[{event_filename}]" if event_filename else ""

        # Formato final del mensaje de log
        log_message = f"{record_time} {level_name} {file_path} {event_filename_str} - {record.getMessage()}"
        return log_message


# Configuración del logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

logger.propagate = False

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(CustomFormatter())
    logger.addHandler(console_handler)
