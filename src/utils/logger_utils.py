import logging
import os
import json
from pytz import timezone
from datetime import datetime, timezone, timedelta
from colorama import Fore, Style
from dotenv import load_dotenv

load_dotenv()

LOG_COLORS = {
    "INFO": Fore.GREEN,
    "ERROR": Fore.RED,
    "WARNING": Fore.YELLOW,
    "DEBUG": Fore.BLUE
}


class CustomFormatter(logging.Formatter):
    def __init__(self, use_json=False):
        super().__init__()
        self.use_json = use_json

    def format(self, record):
        # Configuración de tiempo
        colombia_tz = timezone("America/Bogota")
        record_time = datetime.now(colombia_tz).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Obtener ruta relativa del módulo
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        module_name = os.path.relpath(record.pathname, start=project_root)

        # Construcción del log ordenado
        log_data = {
            "timestamp": record_time,
            "level": record.levelname,
            "module_name": module_name,
            "line_number": record.lineno,
        }

        # Agregar file_name si está presente
        if hasattr(record, "event_filename") and record.event_filename:
            log_data["request_id"] = record.event_filename

        # Agregar el mensaje al final
        log_data["message"] = record.getMessage()

        if self.use_json:
            # Retornar JSON compacto (una sola línea)
            return json.dumps(log_data, ensure_ascii=False)
        else:
            # Formato legible en texto
            level_color = LOG_COLORS.get(record.levelname, "")
            level_name = f"{level_color}[{record.levelname}]{Style.RESET_ALL}"

            request_id_str = f"[{log_data['request_id']}]" if log_data.get('request_id') else ""

            formatted_log = (
                f"{record_time} {level_name} "
                f"{log_data['module_name']}:{log_data['line_number']} "
                f"{request_id_str} "
                f"- {log_data['message']}"
            )
            return formatted_log


def get_logger(debug_mode: bool):
    logger = logging.getLogger(__name__)
    if debug_mode:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    logger.propagate = False

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        # Leer configuración de formato desde variable de entorno
        use_json = os.getenv("LOG_FORMAT", "STRING").upper() == "JSON"
        console_handler.setFormatter(CustomFormatter(use_json=use_json))
        logger.addHandler(console_handler)

    return logger
