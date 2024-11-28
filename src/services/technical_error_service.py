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

logger = get_logger(env.DEBUG_MODE)


class TechnicalErrorService:
    def __init__(self, db: Session, ):
        self.archivo_repository = ArchivoRepository(db)
        self.rta_procesamiento_repository = RtaProcesamientoRepository(db)

    def handle_technical_error(
            self,
            event,
            code_error,
            detail_error,
            acg_nombre_archivo,
            file_id_and_response_processing_id_in_event,

    ):
        id_archivo = self.archivo_repository.get_archivo_by_nombre_archivo(acg_nombre_archivo).id_archivo
        if file_id_and_response_processing_id_in_event:
            if acg_nombre_archivo.startswith(env.CONST_PRE_GENERAL_FILE):
                # si el archivo es general, se debe construir el acg_nombre_archivo.
                acg_nombre_archivo = build_acg_name_if_general_file(acg_nombre_archivo)

            else:
                # quiere decir que el archivo es especial
                acg_nombre_archivo = acg_nombre_archivo

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
