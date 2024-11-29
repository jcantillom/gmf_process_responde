from src.core.process_event import (
    extract_filename_from_body,
    extract_bucket_from_body,
    build_acg_name_if_general_file,
    extract_and_validate_event_data,
)
from src.repositories.archivo_repository import ArchivoRepository
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository
from src.utils.logger_utils import get_logger
from src.config.config import env
from sqlalchemy.orm import Session
from src.repositories.archivo_estado_repository import ArchivoEstadoRepository
from src.services.error_handling_service import ErrorHandlingService
from src.utils.sqs_utils import send_message_to_sqs_with_delay
import json
from datetime import datetime
from src.utils.sqs_utils import delete_message_from_sqs

logger = get_logger(env.DEBUG_MODE)


class TechnicalErrorService:
    def __init__(self, db: Session, ):
        self.archivo_repository = ArchivoRepository(db)
        self.rta_procesamiento_repository = RtaProcesamientoRepository(db)
        self.archivo_estado_repository = ArchivoEstadoRepository(db)
        self.error_handling_service = ErrorHandlingService(db)

    def handle_technical_error(
            self,
            event,
            code_error,
            detail_error,
            acg_nombre_archivo,
            file_id_and_response_processing_id_in_event,
            max_retries,
            retry_delay,
            estado_inicial,
            receipt_handle,
            file_name,
    ):
        id_archivo = self.archivo_repository.get_archivo_by_nombre_archivo(acg_nombre_archivo).id_archivo
        if file_id_and_response_processing_id_in_event:
            if acg_nombre_archivo.startswith(env.CONST_PRE_GENERAL_FILE):
                # si el archivo es general, se debe construir el acg_nombre_archivo.
                acg_nombre_archivo = build_acg_name_if_general_file(acg_nombre_archivo)
            # ============================================================
            #       Insertar error en la tabla CGD_RTA_PROCESAMIENTO
            # ============================================================
            id_rta_procesamiento = \
                extract_and_validate_event_data(event, required_keys=["response_processing_id"])[0][
                    "response_processing_id"
                ]

            self.rta_procesamiento_repository.insert_code_error(
                id_archivo=id_archivo,
                rta_procesamiento_id=id_rta_procesamiento,
                code_error=code_error,
                detail_error=detail_error,
            )
        else:
            # ============================================================
            #       Insertar error en la tabla CGD_ARCHIVO
            # ============================================================
            self.archivo_repository.insert_code_error(
                id_archivo=id_archivo,
                code_error=code_error,
                detail_error=detail_error,
            )

        # ============================================================
        #                   Validar reintento
        # ============================================================
        dody_dict = {}
        for record in event.get("Records", []):
            body = record.get("body", "{}")
            try:
                body_dict = json.loads(body)
            except json.JSONDecodeError as e:
                logger.error(f"Error al decodificar el body del evento: {e}")
                continue
        retry_count = dody_dict.get("retry_count", 0) + 1

        if retry_count < max_retries:
            logger.info("Reenviando mensaje a la cola con un retraso de 15 minutos.")
            # ============================================================
            #     insertar nuevo estado en cgd_archivo_estado
            # ============================================================
            self.archivo_estado_repository.insert_estado_archivo(
                id_archivo=id_archivo,
                estado_inicial=estado_inicial,
                estado_final=env.CONST_ESTADO_PROCESA_PENDIENTE_REINTENTO,
                fecha_cambio_estado=datetime.now(),
            )

            # ============================================================
            #       Actualizar estado del archivo en la tabla CGD_ARCHIVO
            # ============================================================
            self.archivo_repository.update_estado_archivo(
                nombre_archivo=acg_nombre_archivo,
                estado=env.CONST_ESTADO_PROCESA_PENDIENTE_REINTENTO,
                contador_intentos_cargue=retry_count,
            )
            new_body = {
                **body_dict,
                "retry_count": retry_count,
                "is_reprocessing": True,
            }
            new_message = {
                **event,
                "body": json.dumps(new_body),
            }

            send_message_to_sqs_with_delay(
                queue_url=env.SQS_URL_PRO_RESPONSE_TO_PROCESS,
                message_body=new_message,
                filename=file_name,
                delay_seconds=900,
            )
            if receipt_handle:
                delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS, file_name)


        else:
            logger.info("Se ha alcanzado el número máximo de reintento.")
            # ============================================================
            #            INSERTAR NUEVO ESTADO en cgd_archivo_estado
            # ============================================================

            self.archivo_estado_repository.insert_estado_archivo(
                id_archivo=id_archivo,
                estado_inicial=estado_inicial,
                estado_final=env.CONST_ESTADO_PROCESAMIENTO_FALLIDO,
                fecha_cambio_estado=datetime.now(),
            )
            # ============================================================
            #       Actualizar estado del archivo en la tabla CGD_ARCHIVO
            # ============================================================
            self.archivo_repository.update_estado_archivo(
                nombre_archivo=acg_nombre_archivo,
                estado=env.CONST_ESTADO_PROCESAMIENTO_FALLIDO,
                contador_intentos_cargue=retry_count,
            )
            # ============================================================
            #  validar si viene el id_rta_procesamiento para actualizar
            # ============================================================
            if file_id_and_response_processing_id_in_event:
                self.rta_procesamiento_repository.update_state_rta_procesamiento(
                    id_archivo=id_archivo,
                    estado=env.CONST_ESTADO_PROCESAMIENTO_FALLIDO,
                )

            # ============================================================
            #             Manejo de Error
            # ============================================================
            self.error_handling_service.handle_generic_error(
                id_archivo=id_archivo,
                filekey=acg_nombre_archivo,
                bucket_name=extract_bucket_from_body(event),
                receipt_handle=event.get("receipt_handle", ""),
                file_name=extract_filename_from_body(event),
                contador_intentos_cargue=retry_count,
                codigo_error=code_error,
                id_plantilla=env.CONST_ID_PLANTILLA_ERROR_TECHNICAL,
            )
