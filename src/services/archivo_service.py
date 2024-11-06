from datetime import datetime
from src.repositories.archivo_repository import ArchivoRepository
from src.utils.s3_utils import S3Utils
from src.utils.event_utils import (
    extract_filename_from_body,
    extract_bucket_from_body,
    extract_date_from_filename,
    create_file_id,
    create_filename_without_prefix_and_extension,
)
from src.utils.sqs_utils import delete_message_from_sqs, send_message_to_sqs
from src.utils.validator_utils import ArchivoValidator
from src.logs.logger import get_logger
from sqlalchemy.orm import Session
from src.config.config import env
from .error_handling_service import ErrorHandlingService
from src.repositories.archivo_estado_repository import ArchivoEstadoRepository
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository
from ..models.cgd_archivo import CGDArchivo

logger = get_logger(env.DEBUG_MODE)


class ArchivoService:
    def __init__(self, db: Session):
        self.db = db
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

            if not self.validar_evento(filename, bucket, receipt_handle):
                continue

            file_key = f"{env.DIR_RECEPTION_FILES}/{filename}"

            if not self.validar_existencia_archivo_s3(bucket, file_key, filename, receipt_handle):
                continue

            if self.archivo_validator.is_special_prefix(filename):
                if not self.procesar_archivo_especial(bucket, file_key, filename, receipt_handle):
                    continue
            else:
                if not self.procesar_archivo_general(bucket, file_key, filename, receipt_handle):
                    continue

    def validar_evento(self, filename, bucket, receipt_handle):
        """ Valida si el evento contiene el nombre del archivo y el bucket. """
        if not filename or not bucket:
            logger.error("El evento no contiene el nombre del archivo o el bucket",
                         extra={"event_filename": "No filename"})
            delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
            return False
        return True

    def validar_existencia_archivo_s3(self, bucket, file_key, filename, receipt_handle):
        """ Valida si el archivo existe en el bucket S3. """
        if not self.s3_utils.check_file_exists_in_s3(bucket, file_key):
            logger.error("El archivo no existe en el bucket",
                         extra={"event_filename": filename})
            delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
            return False
        return True

    def procesar_archivo_especial(self, bucket, file_key, filename, receipt_handle):
        """ Procesa archivos especiales y ejecuta la lógica respectiva. """
        if not self.archivo_validator.is_special_file(filename):
            self.registrar_error_archivo(filename, file_key, bucket, receipt_handle,
                                         "El nombre del archivo especial no cumple con el formato esperado")
            return False

        acg_nombre_archivo = create_filename_without_prefix_and_extension(filename)
        if self.archivo_repository.check_special_file_exists(acg_nombre_archivo, "05"):
            return self.validar_estado_archivo_y_procesar(bucket, file_key, filename, acg_nombre_archivo,
                                                          receipt_handle)

        return self.insertar_archivo_nuevo_especial(bucket, file_key, filename, acg_nombre_archivo, receipt_handle)

    def procesar_archivo_general(self, bucket, file_key, filename, receipt_handle):
        """ Procesa archivos generales y ejecuta la lógica respectiva. """
        acg_nombre_archivo = self.archivo_validator.build_acg_nombre_archivo(filename)
        if not self.archivo_repository.check_file_exists(acg_nombre_archivo):
            self.registrar_error_archivo(filename, file_key, bucket, receipt_handle,
                                         "El archivo no existe en la base de datos")
            return False

        return self.validar_estado_archivo_y_procesar(bucket, file_key, filename, acg_nombre_archivo, receipt_handle)

    def registrar_error_archivo(self, filename, file_key, bucket, receipt_handle, mensaje_error):
        """ Maneja errores relacionados con el archivo. """
        self.error_handling_service.handle_file_error(
            id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
            filekey=file_key,
            bucket=bucket,
            receipt_handle=receipt_handle,
            codigo_error=env.CONST_COD_ERROR_EMAIL,
            filename=filename,
        )
        logger.error(mensaje_error, extra={"event_filename": filename})
        delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)

    def validar_estado_archivo_y_procesar(self, bucket, file_key, filename, acg_nombre_archivo, receipt_handle):
        """ Valida el estado del archivo y lo procesa si es válido. """
        estado_archivo = self.archivo_repository.get_archivo_by_nombre_archivo(acg_nombre_archivo).estado
        if not self.archivo_validator.is_valid_state(estado_archivo):
            self.registrar_error_archivo(filename, file_key, bucket, receipt_handle,
                                         "El estado del archivo no es válido")
            return False

        self.procesar_archivo_valido(bucket, file_key, filename, acg_nombre_archivo, estado_archivo, receipt_handle)
        return True

    def insertar_archivo_nuevo_especial(self, bucket, file_key, filename, acg_nombre_archivo, receipt_handle):
        """ Inserta un nuevo archivo especial en la base de datos y continúa con el procesamiento. """
        new_archivo = CGDArchivo(
            id_archivo=create_file_id(filename),
            acg_nombre_archivo=acg_nombre_archivo,
            tipo_archivo="05",
            estado=env.CONST_ESTADO_SEND,
            plataforma_origen="01",
            fecha_nombre_archivo=extract_date_from_filename(filename),
            fecha_recepcion=datetime.now(),
            contador_intentos_cargue=0,
            contador_intentos_generacion=0,
            contador_intentos_empaquetado=0,
            nombre_archivo=filename.rsplit(".", 1)[0],
            consecutivo_plataforma_origen=1,
            fecha_ciclo=datetime.now(),
            fecha_registro_resumen=extract_date_from_filename(filename),
        )
        self.archivo_repository.insert_archivo(new_archivo)
        return self.procesar_archivo_valido(bucket, file_key, filename, acg_nombre_archivo, env.CONST_ESTADO_SEND,
                                            receipt_handle)

    def procesar_archivo_valido(self, bucket, file_key, filename, acg_nombre_archivo, estado_archivo, receipt_handle):
        """ Procesa el archivo cuando su estado es válido. """
        new_file_key = self.s3_utils.move_file_to_procesando(bucket, file_key)
        archivo_id = self.archivo_repository.get_archivo_by_nombre_archivo(acg_nombre_archivo).id_archivo
        fecha_cambio_estado = datetime.now()
        new_counter = self.incrementar_intentos_cargue(archivo_id)

        self.estado_archivo_repository.insert_estado_archivo(
            id_archivo=int(archivo_id),
            estado_inicial=estado_archivo,
            estado_final=env.CONST_ESTADO_LOAD_RTA_PROCESSING,
            fecha_cambio_estado=fecha_cambio_estado
        )

        self.rta_procesamiento_repository.insert_rta_procesamiento(
            id_archivo=int(archivo_id),
            nombre_archivo_zip=filename,
            tipo_respuesta=self.archivo_validator.get_type_response(filename),
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

        self.manejar_estado_final(archivo_id, filename, receipt_handle)

    def incrementar_intentos_cargue(self, archivo_id):
        """ Incrementa el contador de intentos de carga para el archivo. """
        last_counter = self.rta_procesamiento_repository.get_last_contador_intentos_cargue(int(archivo_id))
        return last_counter + 1

    def manejar_estado_final(self, archivo_id, filename, receipt_handle):
        """ Maneja el estado final del archivo en CGD_RTA_PROCESAMIENTO. """
        if self.rta_procesamiento_repository.is_estado_enviado(int(archivo_id), filename):
            delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
        else:
            message_body = {
                "file_id": int(archivo_id),
                "response_processing_id": int(
                    self.rta_procesamiento_repository.get_id_rta_procesamiento_by_id_archivo(
                        int(archivo_id),
                        filename
                    )
                )
            }
            send_message_to_sqs(env.SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE, message_body, filename)
            self.rta_procesamiento_repository.update_state_rta_procesamiento(
                id_archivo=int(archivo_id),
                estado=env.CONST_ESTADO_SEND
            )
        delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
