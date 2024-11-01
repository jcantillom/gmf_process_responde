from .config import env
from src.connection.database import DataAccessLayer
from src.controllers.sqs_controller import process_sqs_message
from src.logs.logger import get_logger

logger = get_logger(env.DEBUG_MODE)

if env.APP_ENV == "local":
    from local.load_event import load_local_event, logger


def initialize_lambda(event, context):
    """
    Inicializa la Lambda y procesa el mensaje.
    """
    # Cargar el evento local si estamos en entorno de desarrollo
    if env.APP_ENV == "local":
        event = load_local_event()

    # Inicializar la conexión a la base de datos y procesar el mensaje
    dal = DataAccessLayer()
    with dal.session_scope() as session:
        process_sqs_message(event, session)

    logger.info("Proceso de Lambda completado")
