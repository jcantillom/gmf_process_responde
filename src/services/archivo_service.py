from src.repositories.archivo_repository import ArchivoRepository
from src.utils.s3_utils import S3Utils
from src.utils.event_utils import extract_filename_from_body, extract_bucket_from_body
from src.utils.sqs_utils import delete_message_from_sqs, send_message_to_sqs
from src.utils.validator_utils import ArchivoValidator
from src.logs.logger import get_logger
from sqlalchemy.orm import Session
from src.config.config import env
from .error_handling_service import ErrorHandlingService
from src.repositories.archivo_estado_repository import ArchivoEstadoRepository
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository

logger = get_logger(env.DEBUG_MODE)


class ArchivoService:
    def __init__(self, db: Session):
        self.s3_utils = S3Utils(db)
        self.archivo_validator = ArchivoValidator()
        self.archivo_repository = ArchivoRepository(db)
        self.error_handling_service = ErrorHandlingService(db)
        self.estado_archivo_repository = ArchivoEstadoRepository(db)
        self.rta_procesamiento_repository = RtaProcesamientoRepository(db)

    def validar_y_procesar_archivo(self, event):
        for record in event.get("Records", []):
            receipt_handle = record.get("receiptHandle")
            body = record.get("body", "{}")
            filename = extract_filename_from_body(body)
            bucket = extract_bucket_from_body(body)

            if not self.validar_evento(receipt_handle, filename, bucket):
                continue

            file_key = f"{env.DIR_RECEPTION_FILES}/{filename}"

            if not self.validar_archivo_en_bucket(receipt_handle, filename, bucket, file_key):
                continue

            if self.archivo_validator.is_special_prefix(filename):
                self.procesar_archivo_especial(receipt_handle, filename, bucket, file_key)
            else:
                self.procesar_archivo_general(receipt_handle, filename, bucket, file_key)

    @staticmethod
    def validar_evento(receipt_handle, filename, bucket):
        if not filename or not bucket:
            logger.error("El evento no contiene el nombre del archivo o el bucket",
                         extra={"event_filename": "No filename"})
            delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
            return False
        return True

    def validar_archivo_en_bucket(self, receipt_handle, filename, bucket, file_key):
        if not self.s3_utils.check_file_exists_in_s3(bucket, file_key):
            logger.error("El archivo no existe en el bucket",
                         extra={"event_filename": filename})
            delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
            return False
        return True

    def procesar_archivo_especial(self, receipt_handle, filename, bucket, file_key):
        logger.debug(f"Procesando archivo especial {filename}", extra={"event_filename": filename})

        if not self.archivo_validator.is_special_file(filename):
            self.handle_error(receipt_handle, file_key, bucket, filename,
                              "El nombre del archivo especial no cumple con el formato esperado")
            return

        if not self.archivo_repository.check_special_file_exists(filename, "05"):
            logger.debug(f"El archivo especial {filename} no existe en la base de datos",
                         extra={"event_filename": filename})
            return

        estado_archivo = self.archivo_repository.get_archivo_by_nombre_archivo(filename).estado
        if not self.archivo_validator.is_valid_state(estado_archivo):
            self.handle_error(
                receipt_handle,
                file_key,
                bucket,
                filename,
                "El estado del archivo no es válido",
            )
            return

        self.procesar_archivo(receipt_handle, filename, bucket, file_key, estado_archivo)

    def procesar_archivo_general(self, receipt_handle, filename, bucket, file_key):
        logger.debug(f"El archivo {filename} no es especial, se procesará como Reintento o Nota Débito",
                     extra={"event_filename": filename})

        if not self.archivo_validator.is_general_file(filename):
            self.handle_error(receipt_handle, file_key, bucket, filename,
                              "El archivo no cumple con la estructura esperada")
            return

        acg_nombre_archivo = self.archivo_validator.build_acg_nombre_archivo(filename)
        if not self.archivo_repository.check_file_exists(acg_nombre_archivo):
            self.handle_error(
                receipt_handle,
                file_key,
                bucket,
                filename,
                "El archivo no existe en la base de datos",
            )
            return

        estado_archivo = self.archivo_repository.get_archivo_by_nombre_archivo(acg_nombre_archivo).estado
        if not self.archivo_validator.is_valid_state(estado_archivo):
            self.handle_error(
                receipt_handle,
                file_key,
                bucket,
                filename,
                "El estado del archivo no es válido",
            )
            return

        self.procesar_archivo(receipt_handle, filename, bucket, file_key, estado_archivo)

    def procesar_archivo(self, receipt_handle, filename, bucket, file_key, estado_archivo):
        acg_nombre_archivo = self.archivo_validator.build_acg_nombre_archivo(filename)

        new_file_key = self.s3_utils.move_file_to_procesando(bucket, file_key)
        self.archivo_repository.update_estado_archivo(
            acg_nombre_archivo,
            env.CONST_ESTADO_LOAD_RTA_PROCESSING,
            0)

        archivo_id = self.archivo_repository.get_archivo_by_nombre_archivo(acg_nombre_archivo).id_archivo
        fecha_cambio_estado = self.archivo_repository.get_archivo_by_nombre_archivo(acg_nombre_archivo).fecha_recepcion
        last_counter = self.rta_procesamiento_repository.get_last_contador_intentos_cargue(int(archivo_id))
        new_counter = last_counter + 1

        self.estado_archivo_repository.insert_estado_archivo(
            id_archivo=int(archivo_id),
            estado_inicial=estado_archivo,
            estado_final=env.CONST_ESTADO_LOAD_RTA_PROCESSING,
            fecha_cambio_estado=fecha_cambio_estado
        )

        type_response = self.archivo_validator.get_type_response(filename)
        self.rta_procesamiento_repository.insert_rta_procesamiento(
            id_archivo=int(archivo_id),
            nombre_archivo_zip=filename,
            tipo_respuesta=type_response,
            estado=env.CONST_ESTADO_INICIADO,
            contador_intentos_cargue=new_counter
        )

        self.s3_utils.unzip_file_in_s3(
            bucket,
            new_file_key,
            int(archivo_id),
            acg_nombre_archivo,
            contador_intentos_cargue=new_counter,
            receipt_handle=receipt_handle,
            error_handling_service=self.error_handling_service
        )

        self.finalizar_procesamiento(receipt_handle, archivo_id, filename)

    def finalizar_procesamiento(self, receipt_handle, archivo_id, filename):
        if self.rta_procesamiento_repository.is_estado_enviado(int(archivo_id), filename):
            delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
        else:
            message_body = {
                "file_id": int(archivo_id),
                "response_processing_id": int(
                    self.rta_procesamiento_repository.get_id_rta_procesamiento_by_id_archivo(
                        int(archivo_id), filename))
            }
            send_message_to_sqs(env.SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE, message_body, filename)
            self.rta_procesamiento_repository.update_state_rta_procesamiento(
                id_archivo=int(archivo_id),
                estado=env.CONST_ESTADO_SEND
            )
        delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)

    def handle_error(self, receipt_handle, file_key, bucket, filename, error_message):
        self.error_handling_service.handle_file_error(
            id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
            filekey=file_key,
            bucket=bucket,
            receipt_handle=receipt_handle,
            codigo_error=env.CONST_COD_ERROR_EMAIL,
            filename=filename,
        )
        logger.error(error_message, extra={"event_filename": filename})
