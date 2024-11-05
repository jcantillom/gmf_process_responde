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
            # ======================================================================
            #    VALIDATION SI EL EVENTO CONTIENE EL NOMBRE DEL ARCHIVO Y EL BUCKET
            # ======================================================================
            acg_nombre_archivo = self.archivo_validator.build_acg_nombre_archivo(filename)
            if not filename or not bucket:
                logger.error("El evento no contiene el nombre del archivo o el bucket",
                             extra={"event_filename": "No filename"})
                delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
                return
            # ==============================================================
            #          VALIDATION SI EL ARCHIVO EXISTE EN EL BUCKET
            # ==============================================================
            file_key = f"{env.DIR_RECEPTION_FILES}/{filename}"
            if not self.s3_utils.check_file_exists_in_s3(bucket, file_key):
                logger.error("El archivo no existe en el bucket",
                             extra={"event_filename": filename})
                delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
                return
            # ==============================================================
            #            VALIDATION SI EL ARCHIVO ES ESPECIAL
            # ==============================================================
            # Verificar si el archivo tiene prefijo especial
            if self.archivo_validator.is_special_prefix(filename):
                # Validar la estructura si es especial
                if not self.archivo_validator.is_special_file(filename):

                    self.error_handling_service.handle_file_error(
                        id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
                        filekey=file_key,
                        bucket=bucket,
                        receipt_handle=receipt_handle,
                        codigo_error=env.CONST_COD_ERROR_EMAIL,
                        filename=filename,
                    )
                    logger.error("El nombre del archivo especial no cumple con el formato esperado",
                                 extra={"event_filename": filename})
                    return
                else:
                    logger.debug(f"El archivo especial {filename} cumple con el formato.",
                                 extra={"event_filename": filename})

                    # TODO: Procesar archivo especial
                    if self.archivo_repository.check_special_file_exists(filename, "05"):
                        logger.debug(f"El archivo especial {filename} existe en la base de datos",
                                     extra={"event_filename": filename})
                        #    VALIDATION SI EL ESTADO DEL ARCHIVO ES VÁLIDO
                        estado_archivo = self.archivo_repository.get_archivo_by_nombre_archivo(filename).estado
                        if not self.archivo_validator.is_valid_state(estado_archivo):
                            self.error_handling_service.handle_file_error(
                                id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
                                filekey=file_key,
                                bucket=bucket,
                                receipt_handle=receipt_handle,
                                codigo_error=env.CONST_COD_ERROR_EMAIL,
                                filename=filename,
                            )
                            logger.error("El estado del archivo no es válido",
                                         extra={"event_filename": filename})
                            return
                        else:
                            # Mover archivo a la carpeta de Procesando y obtener el nuevo path de Procesando
                            new_file_key = self.s3_utils.move_file_to_procesando(bucket, file_key)

                            # Actualizar estado del archivo a 'Procesando'
                            self.archivo_repository.update_estado_archivo(
                                acg_nombre_archivo,
                                env.CONST_ESTADO_LOAD_RTA_PROCESSING,
                                0)

                            # ==========================================================================
                            #    INSERTAR ESTADO DEL ARCHIVO EN LA BASE DE DATOS (CGD_ARCHIVO_ESTADOS)
                            # ==========================================================================
                            # values
                            archivo_id = self.archivo_repository.get_archivo_by_nombre_archivo(
                                acg_nombre_archivo).id_archivo
                            fecha_cambio_estado = self.archivo_repository.get_archivo_by_nombre_archivo(
                                acg_nombre_archivo).fecha_recepcion
                            last_counter = self.rta_procesamiento_repository.get_last_contador_intentos_cargue(
                                int(archivo_id))
                            new_counter = last_counter + 1

                            self.estado_archivo_repository.insert_estado_archivo(
                                id_archivo=int(archivo_id),
                                estado_inicial=estado_archivo,
                                estado_final=env.CONST_ESTADO_LOAD_RTA_PROCESSING,
                                fecha_cambio_estado=fecha_cambio_estado
                            )

                            # ===================================================================================
                            #  INSERTAR RESPUESTA DE PROCESAMIENTO EN LA BASE DE DATOS (CGD_RTA_PROCESAMIENTO)
                            # ===================================================================================
                            type_response = self.archivo_validator.get_type_response(filename)
                            self.rta_procesamiento_repository.insert_rta_procesamiento(
                                id_archivo=int(archivo_id),
                                nombre_archivo_zip=filename,
                                tipo_respuesta=type_response,
                                estado=env.CONST_ESTADO_INICIADO,
                                contador_intentos_cargue=new_counter
                            )

                            # ================================================================================
                            #    DESCOMPRIMIR ARCHIVO DESDE LA CARPETA DE PROCESANDO Y REALIZAR VALIDACIONES
                            # ================================================================================
                            self.s3_utils.unzip_file_in_s3(
                                bucket,
                                new_file_key,
                                int(archivo_id),
                                acg_nombre_archivo,
                                contador_intentos_cargue=new_counter,
                                receipt_handle=receipt_handle,
                                error_handling_service=self.error_handling_service
                            )

                            # ===============================================================================
                            #                  OBTENER EL ESTADO DE CGD_RTA_PROCESAMIENTO
                            # ===============================================================================
                            if self.rta_procesamiento_repository.is_estado_enviado(int(archivo_id), filename):
                                delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
                            else:
                                message_body = {
                                    "file_id": int(archivo_id),
                                    "response_processing_id": int(
                                        self.rta_procesamiento_repository.get_id_rta_procesamiento_by_id_archivo(
                                            int(archivo_id),
                                            filename)
                                    )
                                }
                                send_message_to_sqs(env.SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE, message_body, filename)
                                self.rta_procesamiento_repository.update_state_rta_procesamiento(
                                    id_archivo=int(archivo_id),
                                    estado=env.CONST_ESTADO_SEND
                                )
                            delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS,
                                                    filename)


            # ========================================================================
            #                    VALIDATION SI EL ARCHIVO ES GENERAL
            # ========================================================================
            else:
                # Si no es especial, se procesa como reintento o nota de débito
                logger.debug(f"El archivo {filename} no es especial "
                             f"se procesara como Reintento "
                             f"o Nota Debito"
                             , extra={"event_filename": filename})

                # Validar la estructura si es general
                if self.archivo_validator.is_general_file(filename):
                    # validamos que el archivo existe en la base de datos

                    # ================================================================
                    #    VALIDATION SI EL ARCHIVO EXISTE EN LA BASE DE DATOS
                    # ===============================================================
                    if not self.archivo_repository.check_file_exists(acg_nombre_archivo):
                        self.error_handling_service.handle_file_error(
                            id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
                            filekey=file_key,
                            bucket=bucket,
                            receipt_handle=receipt_handle,
                            codigo_error=env.CONST_COD_ERROR_EMAIL,
                            filename=filename,
                        )
                        logger.error("El archivo no existe en la base de datos",
                                     extra={"event_filename": filename})
                        return
                    # Si existe en la base de datos...😊
                    else:
                        logger.debug(f"El archivo general {filename} existe en la base de datos",
                                     extra={"event_filename": filename})

                        # ====================================================
                        #    VALIDATION SI EL ESTADO DEL ARCHIVO ES VÁLIDO
                        # =====================================================

                        estado_archivo = self.archivo_repository.get_archivo_by_nombre_archivo(
                            acg_nombre_archivo).estado

                        if not self.archivo_validator.is_valid_state(estado_archivo):
                            self.error_handling_service.handle_file_error(
                                id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
                                filekey=file_key,
                                bucket=bucket,
                                receipt_handle=receipt_handle,
                                codigo_error=env.CONST_COD_ERROR_EMAIL,
                                filename=filename,
                            )
                            logger.error("El estado del archivo no es válido",
                                         extra={"event_filename": filename})
                            return
                        # Si el estado es válido...😊
                        else:
                            logger.debug(f"El estado del archivo {filename} es válido",
                                         extra={"event_filename": filename})

                            # Mover archivo a la carpeta de Procesando y obtener el nuevo path de Procesando
                            new_file_key = self.s3_utils.move_file_to_procesando(bucket, file_key)

                            # Actualizar estado del archivo a 'Procesando'
                            self.archivo_repository.update_estado_archivo(
                                acg_nombre_archivo,
                                env.CONST_ESTADO_LOAD_RTA_PROCESSING,
                                0)

                            # ==========================================================================
                            #    INSERTAR ESTADO DEL ARCHIVO EN LA BASE DE DATOS (CGD_ARCHIVO_ESTADOS)
                            # ==========================================================================
                            # values
                            archivo_id = self.archivo_repository.get_archivo_by_nombre_archivo(
                                acg_nombre_archivo).id_archivo
                            fecha_cambio_estado = self.archivo_repository.get_archivo_by_nombre_archivo(
                                acg_nombre_archivo).fecha_recepcion
                            last_counter = self.rta_procesamiento_repository.get_last_contador_intentos_cargue(
                                int(archivo_id))
                            new_counter = last_counter + 1

                            self.estado_archivo_repository.insert_estado_archivo(
                                id_archivo=int(archivo_id),
                                estado_inicial=estado_archivo,
                                estado_final=env.CONST_ESTADO_LOAD_RTA_PROCESSING,
                                fecha_cambio_estado=fecha_cambio_estado
                            )

                            # ===================================================================================
                            #  INSERTAR RESPUESTA DE PROCESAMIENTO EN LA BASE DE DATOS (CGD_RTA_PROCESAMIENTO)
                            # ===================================================================================
                            type_response = self.archivo_validator.get_type_response(filename)
                            self.rta_procesamiento_repository.insert_rta_procesamiento(
                                id_archivo=int(archivo_id),
                                nombre_archivo_zip=filename,
                                tipo_respuesta=type_response,
                                estado=env.CONST_ESTADO_INICIADO,
                                contador_intentos_cargue=new_counter
                            )

                            # ================================================================================
                            #    DESCOMPRIMIR ARCHIVO DESDE LA CARPETA DE PROCESANDO Y REALIZAR VALIDACIONES
                            # ================================================================================
                            self.s3_utils.unzip_file_in_s3(
                                bucket,
                                new_file_key,
                                int(archivo_id),
                                acg_nombre_archivo,
                                contador_intentos_cargue=new_counter,
                                receipt_handle=receipt_handle,
                                error_handling_service=self.error_handling_service
                            )

                            # ===============================================================================
                            #                  OBTENER EL ESTADO DE CGD_RTA_PROCESAMIENTO
                            # ===============================================================================
                            if self.rta_procesamiento_repository.is_estado_enviado(int(archivo_id), filename):
                                delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, filename)
                            else:
                                message_body = {
                                    "file_id": int(archivo_id),
                                    "response_processing_id": int(
                                        self.rta_procesamiento_repository.get_id_rta_procesamiento_by_id_archivo(
                                            int(archivo_id),
                                            filename)
                                    )
                                }
                                send_message_to_sqs(env.SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE, message_body, filename)
                                self.rta_procesamiento_repository.update_state_rta_procesamiento(
                                    id_archivo=int(archivo_id),
                                    estado=env.CONST_ESTADO_SEND
                                )
                            delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS,
                                                    filename)
