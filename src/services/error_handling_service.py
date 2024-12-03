from src.repositories.catalogo_error_repository import CatalogoErrorRepository
from src.repositories.correo_parametro_repository import CorreoParametroRepository
from src.utils.sqs_utils import send_message_to_sqs, build_email_message, delete_message_from_sqs
from src.services.s3_service import S3Utils
from src.utils.logger_utils import get_logger
from src.config.config import env
from sqlalchemy.orm import Session
from src.core.validator import ArchivoValidator
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository
from src.repositories.archivo_repository import ArchivoRepository

logger = get_logger(env.DEBUG_MODE)


class ErrorHandlingService:
    def __init__(self, db: Session):
        self.catalogo_error_repository = CatalogoErrorRepository(db)
        self.correo_parametro_repository = CorreoParametroRepository(db)
        self.s3_utils = S3Utils(db)
        self.archivo_validator = ArchivoValidator()
        self.rta_procesamiento_repository = RtaProcesamientoRepository(db)
        self.archivo_repository = ArchivoRepository(db)

    def handle_error_master(
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
        - Válida que el archivo no esté en estado 'procesado'
        - Envía un mensaje de error a la cola de SQS "emails-to-send"
        - Registra el error en el log.
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
            filename=filename
        )
        # Obtener datos del error
        error = self.catalogo_error_repository.get_error_by_code(codigo_error)
        if not error:
            logger.error("Error no encontrado en el catálogo", extra={"codigo_error": codigo_error})
            return
        # ========================================================================================
        #    ENVIAR MENSAJE DE ERROR A LA COLA DE CORREOS SQS, A LA COLA "emails-to-send"
        # ========================================================================================
        # Validar que el archivo no este siendo procesado.
        if not self.archivo_validator.is_not_processed_state(filename):
            logger.error("El archivo se encuentra en estado procesado",
                         extra={"filename": filename})
            return
        logger.debug("Enviando mensaje de error a la cola de correos SQS 'emails-to-send'")

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
        send_message_to_sqs(env.SQS_URL_EMAILS, message, filename)

    def handle_generic_error(
            self,
            id_archivo: int,
            filekey: str,
            bucket_name: str,
            receipt_handle: str,
            file_name: str,
            codigo_error: str,
            id_plantilla: str):
        """
        Maneja errores de archivos y actualiza los estados en la base de datos.
        """
        # Actualiza el estado de CGD_RTA_PROCESAMIENTO a RECHAZADO
        self.rta_procesamiento_repository.update_state_rta_procesamiento(
            id_archivo=id_archivo,
            estado=env.CONST_ESTADO_REJECTED
        )

        # Actualiza el estado del archivo a PROCESAMIENTO_RECHAZADO
        self.archivo_repository.update_estado_archivo(
            file_name,
            env.CONST_ESTADO_PROCESAMIENTO_RECHAZADO,
        )

        # Llama a handle_error_master para enviar el mensaje de error
        self.handle_error_master(
            id_plantilla=id_plantilla,
            filekey=filekey,
            bucket=bucket_name,
            receipt_handle=receipt_handle,
            codigo_error=codigo_error,
            filename=file_name,
        )
