from .config import env
from src.connection.database import DataAccessLayer
from src.controllers.archivo_controller import process_sqs_message
from src.logs.logger import get_logger
from ..utils.sqs_utils import send_message_to_sqs

if env.APP_ENV == "local":
    from local.load_event import load_local_event, logger


def initialize_lambda(event, context):
    """
    Inicializa la Lambda y procesa el mensaje.
    """
    # Obtener el logger dentro de la función para asegurar que use el mock
    log = get_logger(env.DEBUG_MODE)

    # Cargar el evento local si estamos en entorno de desarrollo
    try:
        if env.APP_ENV == "local":
            event = load_local_event()

        # Inicializar la conexión a la base de datos y procesar el mensaje
        dal = DataAccessLayer()
        with dal.session_scope() as session:
            process_sqs_message(event, session)

        log.info("Proceso de Lambda completado")
    except Exception as e:
        log.error(f"Error al inicializar la Lambda: {e}")
        send_message_to_sqs(env.SQS_URL_PRO_RESPONSE_TO_PROCESS, event, "Reintento Fallido")
        raise e
