from sqlalchemy.orm import Session
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.logs.logger import get_logger
from src.config.config import env
from src.repositories.cgd_rta_pro_archivos_repository import CGDRtaProArchivosRepository

logger = get_logger(env.DEBUG_MODE)


class CGDRtaProArchivosService:
    """
    Clase para manejar las operaciones de la tabla 'CGD_RTA_PRO_ARCHIVOS'.
    """

    def __init__(self, db: Session):
        self.db = db
        self.cgd_rta_pro_archivos_repository = CGDRtaProArchivosRepository(db)

    def register_extracted_files(
            self,
            id_archivo: int,
            id_rta_procesamiento: int,
            extracted_files: list[str],
    ):
        """
        Registra los archivos descomprimidos en la tabla CGD_RTA_PRO_ARCHIVOS.
        """
        for file_name in extracted_files:
            tipo_archivo_rta = file_name.rsplit("-", 1)[-1].replace(".txt", "")
            nombre_archivo_txt = file_name.split("/")[-1]

            new_entry = CGDRtaProArchivos(
                id_archivo=id_archivo,
                id_rta_procesamiento=id_rta_procesamiento,
                nombre_archivo=nombre_archivo_txt,
                tipo_archivo_rta=tipo_archivo_rta,
                estado="PENDIENTE_INICIO",
                contador_intentos_cargue=0,
            )
            self.cgd_rta_pro_archivos_repository.insert(new_entry)
            logger.info("Archivo registrado en CGD_RTA_PRO_ARCHIVOS: %s", file_name)

        logger.info("Archivos descomprimidos registrados en CGD_RTA_PRO_ARCHIVOS")
