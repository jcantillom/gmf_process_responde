from .config import env
from src.connection.database import DataAccessLayer
from src.controllers.sqs_controller import process_sqs_message
from src.logs import logger as log

if env.APP_ENV == "local":
    from local.load_event import load_local_event


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

    log.logger.info("Proceso de Lambda completado")
