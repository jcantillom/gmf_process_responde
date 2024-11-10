from datetime import datetime
from src.repositories.archivo_repository import ArchivoRepository
from src.utils.s3_utils import S3Utils
from src.utils.event_utils import (
    extract_filename_from_body,
    extract_bucket_from_body,
    extract_date_from_filename,
    create_file_id,
    extract_consecutivo_plataforma_origen,
    build_acg_name_if_general_file,
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
import sys

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
        # Extraer detalles del evento
        file_name, bucket, receipt_handle, acg_nombre_archivo = (
            self.extract_event_details(event)
        )

        # Validar datos del evento
        if not self.validate_event_data(file_name, bucket, receipt_handle):
            return

        # Validar existencia del archivo en el bucket
        if not self.validate_file_existence_in_bucket(file_name, bucket, receipt_handle):
            return

        # Si el archivo es especial
        if self.archivo_validator.is_special_prefix(file_name):
            self.process_special_file(
                file_name, bucket, receipt_handle, acg_nombre_archivo
            )
        else:
            # Si el archivo es general
            self.process_general_file(
                file_name, bucket, receipt_handle, acg_nombre_archivo
            )

    # Funciones auxiliares

    def extract_event_details(self, event):
        """Extrae los detalles del evento necesarios para el procesamiento."""
        for record in event["Records"]:
            receipt_handle = record.get("receiptHandle")
            body = record.get("body", {})
            file_name = extract_filename_from_body(body)
            bucket_name = extract_bucket_from_body(body)
            acg_nombre_archivo = file_name.split(".")[0]
            return file_name, bucket_name, receipt_handle, acg_nombre_archivo

    def validate_event_data(self, file_name, bucket_name, receipt_handle):
        """Valida que el evento contenga el nombre del archivo y el bucket."""
        if not file_name or not bucket_name:
            delete_message_from_sqs(
                receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, file_name
            )
            logger.error(
                "Nombre de archivo o bucket faltante en el evento; mensaje eliminado."
            )
            return False
        logger.debug(
            f"El evento contiene el nombre del archivo {file_name} y el bucket {bucket_name}."
        )
        return True

    def validate_file_existence_in_bucket(self, file_name, bucket_name, receipt_handle):
        """Valida que el archivo exista en el bucket especificado."""
        file_key = f"{env.DIR_RECEPTION_FILES}/{file_name}"
        if not self.s3_utils.check_file_exists_in_s3(bucket_name, file_key):
            logger.error(
                "El archivo NO existe en el bucket ===> Se eliminara el mensaje de la cola",
                extra={"event_filename": file_name}
            )
            delete_message_from_sqs(
                receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, file_name
            )
            return False
        logger.debug(
            "El archivo SI existe en el bucket",
            extra={"event_filename": file_name}
        )
        return True

    def process_special_file(
            self, file_name, bucket, receipt_handle, acg_nombre_archivo):
        """Proceso de manejo de archivos especiales."""
        if self.archivo_validator.is_special_file(file_name):
            # Verificar si el archivo especial ya existe en la base de datos
            if self.check_existing_special_file(acg_nombre_archivo):
                estado = self.validar_estado_special_file(
                    acg_nombre_archivo, bucket, receipt_handle
                )
                if estado:
                    new_file_key = self.move_file_and_update_state(
                        bucket, file_name, acg_nombre_archivo
                    )
                    self.insert_file_states_and_rta_processing(acg_nombre_archivo, estado, file_name)
                    archivo_id = self.archivo_repository.get_archivo_by_nombre_archivo(
                        acg_nombre_archivo
                    ).id_archivo
                    self.unzip_file(
                        bucket,
                        new_file_key,
                        archivo_id,
                        acg_nombre_archivo,
                        0,
                        receipt_handle,
                        self.error_handling_service,
                    )
                    self.process_sqs_response(archivo_id, file_name, receipt_handle)
            else:
                logger.debug(
                    f"El archivo especial {file_name} no existe en la base de datos."
                )
                self.create_and_process_new_special_file(
                    file_name, acg_nombre_archivo, bucket, receipt_handle
                )
        else:
            self.handle_invalid_special_file(file_name, bucket, receipt_handle)

    def check_existing_special_file(self, acg_nombre_archivo) -> bool:
        """Valida si el archivo especial existe en la base de datos."""
        exists = self.archivo_repository.check_special_file_exists(
            acg_nombre_archivo, env.CONST_TIPO_ARCHIVO_ESPECIAL
        )
        if exists:
            logger.warning(
                f"El archivo especial ya existe en la base de datos",
                extra={"event_filename": acg_nombre_archivo},
            )
        return exists

    def validar_estado_special_file(self, acg_nombre_archivo, bucket, receipt_handle):
        # obtener el estado del archivo especial desde la base de datos
        estado = self.archivo_repository.get_archivo_by_nombre_archivo(
            acg_nombre_archivo
        ).estado
        file_name = acg_nombre_archivo + ".zip"
        if estado:
            if not self.archivo_validator.is_valid_state(estado):
                logger.error(
                    f" El estado {estado} del archivo especial no es válido ",
                    extra={"event_filename": file_name},
                )
                self.error_handling_service.handle_file_error(
                    id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
                    filekey=env.DIR_RECEPTION_FILES + "/" + file_name,
                    bucket=bucket,
                    receipt_handle=receipt_handle,
                    codigo_error=env.CONST_COD_ERROR_EMAIL,
                    filename=file_name,
                )
            else:
                logger.debug(
                    f" El estado {estado} del archivo especial es válido",
                    extra={"event_filename": file_name},
                )
                return estado
        else:
            logger.error(
                f"El archivo especial {file_name} no tiene estado, se elimina el mensaje de la cola"
            )
            sys.exit(1)

    # TODO: Pendiente Eliminar.
    def get_estado_archivo(self, acg_nombre_archivo):
        """Obtiene el estado del archivo."""
        estado = self.archivo_repository.get_archivo_by_nombre_archivo(
            acg_nombre_archivo
        ).estado
        logger.debug(
            f"Cargando estado del archivo {acg_nombre_archivo} en la base de datos."
        )
        return estado

    def move_file_and_update_state(self, bucket, file_name, acg_nombre_archivo):
        """Mueve el archivo y actualiza su estado."""
        new_file_key = self.s3_utils.move_file_to_procesando(bucket, file_name)
        self.archivo_repository.update_estado_archivo(
            acg_nombre_archivo, env.CONST_ESTADO_LOAD_RTA_PROCESSING, 0
        )
        logger.debug(
            f"Se actualiza el estado del archivo a {env.CONST_ESTADO_LOAD_RTA_PROCESSING}",
            extra={"event_filename": file_name},
        )
        return new_file_key

    def insert_file_states_and_rta_processing(self, acg_nombre_archivo, estado, file_name):
        """Inserta los estados del archivo en la base de datos."""
        archivo_id = self.archivo_repository.get_archivo_by_nombre_archivo(
            acg_nombre_archivo
        ).id_archivo
        fecha_cambio_estado = self.archivo_repository.get_archivo_by_nombre_archivo(
            acg_nombre_archivo
        ).fecha_recepcion
        last_counter = (
                self.rta_procesamiento_repository.get_last_contador_intentos_cargue(
                    int(archivo_id)
                )
                + 1
        )

        # Insertar en CGD_ARCHIVO_ESTADOS
        self.estado_archivo_repository.insert_estado_archivo(
            id_archivo=int(archivo_id),
            estado_inicial=estado,
            estado_final=env.CONST_ESTADO_LOAD_RTA_PROCESSING,
            fecha_cambio_estado=fecha_cambio_estado,
        )
        logger.debug(
            f"Se inserta el estado del archivo {file_name} en CGD_ARCHIVO_ESTADOS",
            extra={"event_filename": file_name},
        )

        # Insertar en CGD_RTA_PROCESAMIENTO
        type_response = self.archivo_validator.get_type_response(file_name)
        self.rta_procesamiento_repository.insert_rta_procesamiento(
            id_archivo=int(archivo_id),
            nombre_archivo_zip=file_name,
            tipo_respuesta=type_response,
            estado=env.CONST_ESTADO_INICIADO,
            contador_intentos_cargue=last_counter,
        )
        logger.debug(
            f"Se inserta la respuesta de procesamiento del archivo especial {file_name} en CGD_RTA_PROCESAMIENTO"
        )

    def unzip_file(
            self,
            bucket,
            new_file_key,
            archivo_id,
            acg_nombre_archivo,
            new_counter,
            receipt_handle,
            error_handling_service,
    ):
        """
        Descomprime un archivo
        """
        self.s3_utils.unzip_file_in_s3(
            bucket,
            new_file_key,
            int(archivo_id),
            acg_nombre_archivo,
            new_counter,
            receipt_handle,
            error_handling_service,
        )

    def process_sqs_response(self, archivo_id, file_name, receipt_handle):
        """Manejo de la respuesta SQS."""
        if self.rta_procesamiento_repository.is_estado_enviado(
                int(archivo_id), file_name
        ):
            delete_message_from_sqs(
                receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, file_name
            )
        else:
            message_body = {
                "file_id": int(archivo_id),
                "response_processing_id": int(
                    self.rta_procesamiento_repository.get_id_rta_procesamiento_by_id_archivo(
                        int(archivo_id), file_name
                    )
                ),
            }
            send_message_to_sqs(
                env.SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE, message_body, file_name
            )
            self.rta_procesamiento_repository.update_state_rta_procesamiento(
                id_archivo=int(archivo_id), estado=env.CONST_ESTADO_SEND
            )
        delete_message_from_sqs(
            receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, file_name
        )

    def handle_invalid_special_file(self, file_name, bucket, receipt_handle):
        """Maneja archivos especiales con formato incorrecto."""
        file_key = f"{env.DIR_RECEPTION_FILES}/{file_name}"
        self.error_handling_service.handle_file_error(
            id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
            filekey=file_key,
            bucket=bucket,
            receipt_handle=receipt_handle,
            codigo_error=env.CONST_COD_ERROR_EMAIL,
            filename=file_name,
        )
        logger.error(
            f"Formato de archivo especial {file_name} no válido; mensaje eliminado."
        )

    def process_general_file(
            self, file_name, bucket, receipt_handle, acg_nombre_archivo
    ):
        """Proceso de manejo de archivos generales."""
        # 1. Validar la estructura del nombre del archivo
        if self.archivo_validator.validate_filename_structure_for_general_file(file_name):
            # 2. Construir el acg_nombre_archivo sin el prefijo ni la extensión para poder buscarlo en la base de datos
            acg_nombre_archivo = build_acg_name_if_general_file(acg_nombre_archivo)
            # 3. Verificar si el archivo existe en la base de datos
            if not self.archivo_repository.check_file_exists(acg_nombre_archivo):
                error_message = (
                    "El archivo NO existe en la base de datos\n"
                    "===> Se eliminara el mensaje de la cola ... \n"
                    "===> Se movera el archivo a la carpeta de bucket/Rechazados ...\n"
                    "===> Se Eliminara el archivo del bucket/Recibidos ..."
                )
                logger.error(
                    error_message,
                    extra={"event_filename": file_name}
                )
                self.error_handling_service.handle_file_error(
                    id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
                    filekey=f"{env.DIR_RECEPTION_FILES}/{file_name}",
                    bucket=bucket,
                    receipt_handle=receipt_handle,
                    codigo_error=env.CONST_COD_ERROR_EMAIL,
                    filename=file_name,
                )
                return
            logger.debug(
                f"El archivo existe en la base de datos.",
                extra={"event_filename": file_name}
            )

            # Obtener y validar el estado del archivo
            estado_archivo = self.archivo_repository.get_archivo_by_nombre_archivo(
                acg_nombre_archivo
            ).estado
            if not self.archivo_validator.is_valid_state(estado_archivo):
                logger.error(
                    f"Estado del archivo general no válido: {estado_archivo}",
                    extra={"event_filename": file_name}
                )
                self.error_handling_service.handle_file_error(
                    id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
                    filekey=f"{env.DIR_RECEPTION_FILES}/{file_name}",
                    bucket=bucket,
                    receipt_handle=receipt_handle,
                    codigo_error=env.CONST_COD_ERROR_EMAIL,
                    filename=file_name,
                )
                return

            # Mover el archivo a la carpeta de procesando
            new_file_key = self.move_file_and_update_state(bucket, file_name, acg_nombre_archivo)
            # Insertar estados del archivo en la base de datos y respuesta de procesamiento
            self.insert_file_states_and_rta_processing(acg_nombre_archivo, estado_archivo, file_name)
            # Descomprimir el archivo
            self.unzip_file(
                bucket,
                new_file_key,
                self.archivo_repository.get_archivo_by_nombre_archivo(acg_nombre_archivo).id_archivo,
                acg_nombre_archivo,
                0,
                receipt_handle,
                self.error_handling_service,
            )
            # Procesar la respuesta SQS
            self.process_sqs_response(
                self.archivo_repository.get_archivo_by_nombre_archivo(acg_nombre_archivo).id_archivo,
                file_name,
                receipt_handle,
            )
