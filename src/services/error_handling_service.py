from src.repositories.catalogo_error_repository import CatalogoErrorRepository
from src.repositories.correo_parametro_repository import CorreoParametroRepository
from src.utils.sqs_utils import send_message_to_sqs, build_email_message, delete_message_from_sqs
from src.utils.s3_utils import S3Utils
from src.logs.logger import get_logger
from src.config.config import env
from sqlalchemy.orm import Session
from src.utils.validator_utils import ArchivoValidator
from typing import Dict
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository

logger = get_logger(env.DEBUG_MODE)


class ErrorHandlingService:
    def __init__(self, db: Session):
        self.catalogo_error_repository = CatalogoErrorRepository(db)
        self.correo_parametro_repository = CorreoParametroRepository(db)
        self.s3_utils = S3Utils(db)
        self.archivo_validator = ArchivoValidator()
        self.rta_procesamiento_repository = RtaProcesamientoRepository(db)

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

    def handle_unzip_error(
            self,
            id_archivo: int,
            nombre_archivo: str,
            contador_intentos_cargue: int,
            file_key: str,
            bucket_name: str,
            receipt_handle: str):

        """
        Realiza el manejo completo de un error de descompresión:
        :param id_plantilla:
        :param filekey:
        :param bucket:
        :param receipt_handle:
        :param codigo_error:
        :param filename:
        :return:
        """

        # actualiza el estado de CGD_RTA_PROCESAMIENTO a RECHAZADO
        self.rta_procesamiento_repository.update_state_rta_procesamiento(
            id_archivo=id_archivo,
            estado=env.CONST_ESTADO_REJECTED
        )

        # actualiza el estado del archivo a PROCESAMIENTO_RECHAZADO
        self.archivo_repository.update_estado_archivo(
            nombre_archivo,
            env.CONST_ESTADO_PROCESAMIENTO_RECHAZADO,
            contador_intentos_cargue=contador_intentos_cargue,
        )

        # llamamos al error_handling para enviar el mensaje de error
        self.handle_file_error(
            id_plantilla=env.CONST_ID_PLANTILLA_CORREO_ERROR_DECOMPRESION,
            filekey=file_key,
            bucket=bucket_name,
            receipt_handle=receipt_handle,
            codigo_error=env.CONST_COD_ERROR_DECOMPRESION,
            filename=nombre_archivo,
        )
