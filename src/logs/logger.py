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
    def format(self, record):
        colombia_tz = pytz.timezone("America/Bogota")
        record_time = datetime.now(colombia_tz).strftime("%Y-%m-%d %H:%M:%S")
        level_color = LOG_COLORS.get(record.levelname, "")
        level_name = f"{level_color}[{record.levelname}]{Style.RESET_ALL}"

        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        relative_path = os.path.relpath(record.pathname, start=project_root)
        file_path = f"[{relative_path}:{record.lineno}]"

        event_filename = getattr(record, "event_filename", None)
        event_filename_str = f"[{event_filename}]" if event_filename else ""

        log_message = f"{record_time} {level_name} {file_path} {event_filename_str} - {record.getMessage()}"
        return log_message


def get_logger(debug_mode: bool):
    logger = logging.getLogger(__name__)
    if debug_mode:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    logger.propagate = False

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(CustomFormatter())
        logger.addHandler(console_handler)

    return logger
