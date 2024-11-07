from datetime import datetime
from fileinput import filename

from src.repositories.archivo_repository import ArchivoRepository
from src.utils.s3_utils import S3Utils
from src.utils.event_utils import (
    extract_filename_from_body,
    extract_bucket_from_body,
    extract_date_from_filename,
    create_file_id,
    extract_consecutivo_plataforma_origen,
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
        self.s3_utils = S3Utils(db)
        self.archivo_validator = ArchivoValidator()
        self.archivo_repository = ArchivoRepository(db)
        self.error_handling_service = ErrorHandlingService(db)
        self.estado_archivo_repository = ArchivoEstadoRepository(db)
        self.rta_procesamiento_repository = RtaProcesamientoRepository(db)

    def validar_y_procesar_archivo(self, event):

        # =====================================================================================
        #          EXTRAER EL NOMBRE DEL ARCHIVO, EL NOMBRE DEL BUCKET Y EL RECEIPT HANDLE
        # =====================================================================================
        file_name, bucket, receipt_handle, acg_nombre_archivo = (
            self.extract_file_name_and_name_bucket_and_receipt_handle(
                event))

        # Path : Recibidos/file_name
        file_key = f"{env.DIR_RECEPTION_FILES}/{file_name}"
        # ======================================================================
        # VALIDATION SI EL EVENTO CONTIENE EL NOMBRE DEL ARCHIVO Y EL BUCKET
        # ======================================================================
        if not self.validate_if_file_name_and_bucket_name_exists_in_event(file_name, bucket, receipt_handle):
            return

        # ==============================================================
        #          VALIDATION SI EL ARCHIVO EXISTE EN EL BUCKET
        # ==============================================================
        if not self.validate_if_file_exist_in_bucket(file_name, bucket, receipt_handle):
            return

        # ==============================================================
        #            VALIDATION SI EL ARCHIVO ES ESPECIAL
        # ==============================================================
        # Verificar si el archivo tiene prefijo especial
        if self.archivo_validator.is_special_prefix(file_name):
            # Validar la estructura si es especial
            if not self.archivo_validator.is_special_file(file_name):

                """
                - Si el archivo no cumple con la estructura esperada, se mueve a la carpeta de Rechazados, 
                - se elimina elmensaje a la cola "pro-responses-to-process".
                - se registra el log a nivel de error.
                """
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
                # ================================================================
                #       EL ARCHIVO ES ESPECIAL, Y EXISTE EN LA BASE DE DATOS
                # ================================================================
                if self.validate_if_especial_exist_in_database(acg_nombre_archivo):
                    # Validar si el archivo especial tiene un estado válido
                    estado = self.validar_estado_special_file(acg_nombre_archivo, bucket, receipt_handle)
                    if estado:
                        # Mover archivo a la carpeta de Procesando y obtener el nuevo path de Procesando
                        new_file_key = self.s3_utils.move_file_to_procesando(bucket, file_name)

                        # Actualizar estado del archivo a 'CARGANDO_RTA_PROCESAMIENTO'
                        self.archivo_repository.update_estado_archivo(
                            acg_nombre_archivo,
                            env.CONST_ESTADO_LOAD_RTA_PROCESSING,
                            0)

                        logger.debug(f"Se actualiza el estado del archivo especial {file_name} a "
                                     f"{env.CONST_ESTADO_LOAD_RTA_PROCESSING}",
                                     extra={"event_filename": file_name})

                        # ==========================================================================
                        #    INSERTAR ESTADO DEL ARCHIVO EN LA BASE DE DATOS (CGD_ARCHIVO_ESTADOS)
                        # ==========================================================================
                        # obtener el id del archivo
                        archivo_id = self.archivo_repository.get_archivo_by_nombre_archivo(
                            acg_nombre_archivo).id_archivo

                        # Obtener la fecha de recepción del archivo
                        fecha_cambio_estado = self.archivo_repository.get_archivo_by_nombre_archivo(
                            acg_nombre_archivo).fecha_recepcion

                        # Obtener el último contador de intentos de cargue
                        last_counter = self.rta_procesamiento_repository.get_last_contador_intentos_cargue(
                            int(archivo_id))
                        new_counter = last_counter + 1

                        # Insertar el estado del archivo en la base de datos (CGD_ARCHIVO_ESTADOS)
                        self.estado_archivo_repository.insert_estado_archivo(
                            id_archivo=int(archivo_id),
                            estado_inicial=estado,
                            estado_final=env.CONST_ESTADO_LOAD_RTA_PROCESSING,
                            fecha_cambio_estado=fecha_cambio_estado
                        )
                        logger.debug(f"Se inserta el estado del archivo especial {file_name} en CGD_ARCHIVO_ESTADOS",
                                     extra={"event_filename": file_name})
                        # ===================================================================================
                        #  INSERTAR RESPUESTA DE PROCESAMIENTO EN LA BASE DE DATOS (CGD_RTA_PROCESAMIENTO)
                        # ===================================================================================
                        type_response = self.archivo_validator.get_type_response(file_name)
                        self.rta_procesamiento_repository.insert_rta_procesamiento(
                            id_archivo=int(archivo_id),
                            nombre_archivo_zip=file_name,
                            tipo_respuesta=type_response,
                            estado=env.CONST_ESTADO_INICIADO,
                            contador_intentos_cargue=new_counter
                        )
                        logger.debug(f"Se inserta la respuesta de procesamiento del archivo especial {file_name} "
                                     f"en CGD_RTA_PROCESAMIENTO",
                                     extra={"event_filename": file_name})

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
                else:
                    # Si no existe en la base de datos se inserta
                    logger.debug(f"El archivo especial {file_name} no existe en la base de datos, se insertará",
                                 extra={"event_filename": file_name})

                    # Crear nuevo archivo especial
                    file_name_without_extension = file_name.split(".")[0]
                    new_archivo = CGDArchivo(
                        id_archivo=create_file_id(file_name),
                        acg_nombre_archivo=file_name_without_extension,
                        tipo_archivo=env.CONST_TIPO_ARCHIVO_ESPECIAL,
                        consecutivo_plataforma_origen=extract_consecutivo_plataforma_origen(file_name),
                        estado=env.CONST_ESTADO_SEND,
                        plataforma_origen=env.CONST_PLATAFORMA_ORIGEN,
                        fecha_nombre_archivo=extract_date_from_filename(file_name),
                        fecha_registro_resumen=extract_date_from_filename(file_name),
                        fecha_recepcion=datetime.now(),
                        contador_intentos_cargue=0,
                        contador_intentos_generacion=0,
                        contador_intentos_empaquetado=0,
                        nombre_archivo=file_name_without_extension,
                        fecha_ciclo=datetime.now(),
                    )
                    self.archivo_repository.insert_archivo(new_archivo)

                    # ==============================================================================
                    #  SE MUEVE EL ARCHIVO A LA CARPETA DE PROCESANDO Y SE ACTUALIZA EL ESTADO
                    # ==============================================================================
                    new_file_key = self.s3_utils.move_file_to_procesando(bucket, filename)

                    # Actualizar estado del archivo a 'CARGANDO_RTA_PROCESAMIENTO'
                    self.archivo_repository.update_estado_archivo(
                        file_name_without_extension,
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
                acg_nombre_archivo: str = self.archivo_validator.build_acg_nombre_archivo(filename)

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

    def extract_file_name_and_name_bucket_and_receipt_handle(self, event):
        for record in event['Records']:
            receipt_handler = record.get('receiptHandle')
            body = record.get('body', {})
            file_name = extract_filename_from_body(body)
            bucket_name = extract_bucket_from_body(body)
            acg_nombre_archivo = file_name.split(".")[0]

            return file_name, bucket_name, receipt_handler, acg_nombre_archivo

    def validate_if_file_name_and_bucket_name_exists_in_event(self, file_name, bucket_name, receipt_handle):
        if not file_name or not bucket_name:
            delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, file_name)
            logger.error(f"File name or bucket name No Existe en el evento, se elimina el mensaje de la cola")
            return False
        return True

    def validate_if_file_exist_in_bucket(self, file_name, bucket_name, receipt_handle):
        file_key = f"{env.DIR_RECEPTION_FILES}/{file_name}"
        if not self.s3_utils.check_file_exists_in_s3(bucket_name, file_key):
            delete_message_from_sqs(
                receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, file_name
            )
            logger.error(
                f"El archivo {file_key} no existe en el bucket {bucket_name}, se elimina el mensaje de la cola"
            )
            return False
        return True

    def validate_if_especial_exist_in_database(self, acg_nombre_archivo) -> bool:
        if self.archivo_repository.check_special_file_exists(acg_nombre_archivo, env.CONST_TIPO_ARCHIVO_ESPECIAL):
            logger.warning(f"El archivo especial {acg_nombre_archivo}.zip  ya existe en la base de datos")
            return True
        return False

    def validar_estado_special_file(self, acg_nombre_archivo, bucket, receipt_handle):
        # obtener el estado del archivo especial desde la base de datos
        estado = self.archivo_repository.get_archivo_by_nombre_archivo(acg_nombre_archivo).estado
        file_name = acg_nombre_archivo + ".zip"
        if estado:
            if not self.archivo_validator.is_valid_state(estado):
                logger.error(f" 🚫 El estado {estado} del archivo especial {file_name} no es válido 🚫",
                             extra={"event_filename": file_name})
                self.error_handling_service.handle_file_error(
                    id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
                    filekey=env.DIR_RECEPTION_FILES + "/" + file_name,
                    bucket=bucket,
                    receipt_handle=receipt_handle,
                    codigo_error=env.CONST_COD_ERROR_EMAIL,
                    filename=file_name,
                )
                return
            else:
                logger.debug(f" ✅ El estado {estado} del archivo especial {file_name} es válido ✅",
                             extra={"event_filename": file_name})
                return estado
        else:
            logger.error(f"El archivo especial {file_name} no tiene estado, se elimina el mensaje de la cola")
            return
