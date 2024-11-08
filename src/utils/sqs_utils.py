from datetime import datetime
from typing import Dict, List, Any
from src.aws.clients import AWSClients
from src.logs.logger import get_logger
from src.config.config import env
from src.models.cgd_correo_parametro import CGDCorreosParametros
import json

logger = get_logger(env.DEBUG_MODE)


def delete_message_from_sqs(receipt_handle: str, queue_url: str, filename: str):
    """
    Elimina un mensaje de una cola SQS.

    :param receipt_handle: Identificador del mensaje a eliminar.
    :param queue_url: URL de la cola SQS.
    :param filename: Nombre del archivo que generó el evento.
    """
    sqs = AWSClients.get_sqs_client()
    try:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logger.info("Mensaje eliminado de SQS con éxito", extra={"event_filename": filename})
    except Exception as e:
        logger.error("Error al eliminar el mensaje de SQS: %s", e, extra={"event_filename": filename})


def send_message_to_sqs(queue_url: str, message_body: dict, filename: str):
    """
    Envía un mensaje a una cola SQS.

    :param queue_url: URL de la cola SQS.
    :param message_body: Cuerpo del mensaje a enviar.
    :param filename: Nombre del archivo que generó el evento.
    """
    sqs = AWSClients.get_sqs_client()
    try:
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_body, ensure_ascii=False))
        logger.debug("Mensaje enviado a SQS con éxito", extra={"event_filename": filename})
    except Exception as e:
        logger.error("Error al enviar mensaje a SQS: %s", e, extra={"event_filename": filename})


def build_email_message(
        id_plantilla: str,
        error_data: Dict[str, str],
        mail_parameters: List[CGDCorreosParametros],
        filename: str,
) -> dict[str, list[Any] | str]:
    """
    Construye el mensaje de error que se enviará a la cola de correos.
    """
    # Formato de fecha y hora actual
    fecha_formateada = datetime.now().strftime("%d/%m/%Y")
    hora_formateada = datetime.now().strftime("%I:%M %p")

    # Estructura del mensaje
    message = {
        "id_plantilla": id_plantilla,
        "parametros": []
    }

    # Crear estructura de parámetros
    param_values = {
        "codigo_rechazo": error_data.get("codigo_error"),
        "descripcion_rechazo": error_data.get("descripcion"),
        "fecha_recepcion": fecha_formateada,
        "hora_recepcion": hora_formateada,
        "nombre_respuesta_pro_tu": filename,
        "plataforma_origen": "STRATUS"
    }

    for param in mail_parameters:
        message["parametros"].append({
            "nombre": param.id_parametro,
            "valor": param_values.get(param.id_parametro, "")
        })

    return message
