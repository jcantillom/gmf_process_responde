from src.repositories.catalogo_error_repository import CatalogoErrorRepository
from src.repositories.correo_parametro_repository import CorreoParametroRepository
from src.utils.sqs_utils import send_message_to_sqs, build_email_message, delete_message_from_sqs
from src.utils.s3_utils import S3Utils
from src.logs.logger import get_logger
from src.config.config import env
from sqlalchemy.orm import Session
from src.utils.validator_utils import ArchivoValidator
from typing import Dict

logger = get_logger(env.DEBUG_MODE)


class ErrorHandlingService:
    def __init__(self, db: Session):
        self.catalogo_error_repository = CatalogoErrorRepository(db)
        self.correo_parametro_repository = CorreoParametroRepository(db)
        self.s3_utils = S3Utils(db)
        self.archivo_validator = ArchivoValidator()

    def handle_file_error(
            self,
            id_plantilla: str,
            filekey: str,
            bucket: str,
            receipt_handle: str,
            codigo_error: str,
            filename: str):
        """
        Realiza el manejo completo de un error de archivo:
        - Mueve el archivo a 'rechazados'
        - Elimina el mensaje de la cola
        - Envía un mensaje de error a la cola de SQS
        - Registra el error en el log

        Args:
            bucket (str): Nombre del bucket de S3.
            file_key (str): Ruta del archivo en S3.
            receipt_handle (str): Identificador del mensaje en la cola.
            id_plantilla (str): Identificador de la plantilla de correo.
            codigo_error (str): Código del error.
            filename (str): Nombre del archivo relacionado con el error.
        """
        # ================================================================
        #            MOVER EL ARCHIVO A LA CARPETA DE RECHAZADOS
        # ================================================================
        self.s3_utils.move_file_to_rechazados(bucket, filekey)

        # ================================================
        #            ELIMINAR EL MENSAJE DE LA COLA
        # ================================================
        delete_message_from_sqs(
            queue_url=env.SQS_URL_PRO_RESPONSE_TO_PROCESS,
            receipt_handle=receipt_handle,
        )

        # Obtener datos del error
        error = self.catalogo_error_repository.get_error_by_code(codigo_error)
        if not error:
            logger.error("Error no encontrado en el catálogo", extra={"codigo_error": codigo_error})
            return
        # ========================================================================================
        #  ENVIAR MENSAJE DE ERROR A LA COLA DE CORREOS SQS, SI EL ARCHIVO NO HA SIDO PROCESADO
        # ========================================================================================
        if not self.archivo_validator.is_not_processed_state(filename):
            logger.error("El archivo se encuentra en estado procesado",
                         extra={"filename": filename})
            return
        logger.debug("Enviando mensaje de error a la cola de correos SQS 📧")

        # Obtener parámetros de la plantilla
        mail_parameters = self.correo_parametro_repository.get_parameters_by_template(id_plantilla)
        if not mail_parameters:
            logger.error("No se encontraron parámetros para la plantilla de correo",
                         extra={"id_plantilla": id_plantilla})
            return

        # Construir mensaje de error
        error_data = {
            "codigo_error": error.codigo_error,
            "descripcion": error.descripcion,
        }
        message = build_email_message(id_plantilla, error_data, mail_parameters, filename)

        # Enviar mensaje a SQS
        send_message_to_sqs(env.SQS_URL_EMAILS, message)
        logger.info("Mensaje de error enviado a la cola de correos")
