from sqlalchemy.orm import Session
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.logs.logger import get_logger
from src.config.config import env
from src.repositories.cgd_rta_pro_archivos_repository import CGDRtaProArchivosRepository
from src.utils.sqs_utils import send_message_to_sqs

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
                estado=env.CONST_ESTADO_INIT_PENDING,
                contador_intentos_cargue=0,
            )
            self.cgd_rta_pro_archivos_repository.insert(new_entry)

        logger.info("Archivos descomprimidos registrados en CGD_RTA_PRO_ARCHIVOS")

    def send_pending_files_to_queue_by_id(self, id_archivo: int, queue_url: str, destination_folder: str):
        """
        Envía mensajes a la cola para cada archivo de un 'id_archivo' específico en estado 'PENDIENTE_INICIO'.

        :param id_archivo: El ID del archivo para filtrar los registros.
        :param queue_url: La URL de la cola SQS donde se enviarán los mensajes.
        :param destination_folder: La carpeta de destino donde se moverán los archivos.
        """
        pending_files = self.cgd_rta_pro_archivos_repository.get_pending_files_by_id_archivo(id_archivo)

        for file in pending_files:
            message_body = {
                "bucket_name": env.S3_BUCKET_NAME,
                "folder_name": destination_folder.rstrip("/"),
                "file_name": file.nombre_archivo,
                "file_id": int(file.id_archivo),
                "response_processing_id": int(file.id_rta_procesamiento),
            }
            send_message_to_sqs(queue_url, message_body, file.nombre_archivo)

            self.cgd_rta_pro_archivos_repository.update_estado_to_enviado(file.id_archivo, file.nombre_archivo)

        logger.debug(f"Estado actualizado a 'ENVIADO' para archivo con ID {id_archivo}")
