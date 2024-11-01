from src.aws.clients import AWSClients
from src.logs.logger import get_logger
from src.config.config import env

logger = get_logger(env.DEBUG_MODE)


def delete_message_from_sqs(receipt_handle: str, queue_url: str):
    """
    Elimina un mensaje de una cola SQS.

    :param receipt_handle: Identificador del mensaje a eliminar.
    :param queue_url: URL de la cola SQS.
    """
    sqs = AWSClients.get_sqs_client()
    try:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logger.info("Mensaje eliminado de SQS con éxito", extra={"event_filename": "N/A"})
    except Exception as e:
        logger.error("Error al eliminar el mensaje de SQS: %s", e, extra={"event_filename": "N/A"})
