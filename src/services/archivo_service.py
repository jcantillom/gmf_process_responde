from src.repositories.archivo_repository import ArchivoRepository
from src.utils.s3_utils import check_file_exists_in_s3
from src.utils.event_utils import extract_filename_from_body, extract_bucket_from_body
from src.utils.sqs_utils import delete_message_from_sqs
from src.logs.logger import get_logger
from sqlalchemy.orm import Session
from src.config.config import env

logger = get_logger(env.DEBUG_MODE)


class ArchivoService:
    def __init__(self, db: Session):
        self.repository = ArchivoRepository(db)

    def validar_y_procesar_archivo(self, event):
        """
        Válida el mensaje de SQS y verifica la existencia del archivo en S3.
        """
        for record in event.get("Records", []):
            try:
                # Extraer receipt handle para eliminar el mensaje de la cola
                receipt_handle = record.get("receiptHandle")

                # Extrae y valida la información del archivo desde el mensaje
                body = record.get("body", "{}")
                filename = extract_filename_from_body(body)
                bucket = extract_bucket_from_body(body)

                # Log con el nombre del archivo
                logger.info("Procesando archivo", extra={"event_filename": filename})

                # verificar si en el evento viene el nombre del archivo
                if not filename:
                    logger.error("No se encontró el nombre del archivo en el evento",
                                 extra={"event_filename": "No filename"})
                    # Eliminar el mensaje de la cola si no es válido
                    delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS)
                    return

                logger.debug(f"El evento contiene el Nombre del archivo: {filename}")

                file_key = f"Recibidos/{filename}"

                # Verificar que el archivo existe en S3
                if not check_file_exists_in_s3(bucket, file_key):
                    logger.error("El archivo no existe en el bucket", extra={"event_filename": filename})
                    # Eliminar el mensaje de la cola si no es válido
                    delete_message_from_sqs(receipt_handle, env.SQS_URL_PRO_RESPONSE_TO_PROCESS)
                    return

                logger.debug(f"El archivo {filename} existe en el bucket {bucket}")

                # Busca el archivo en la base de datos (si aplica)
                archivo = self.repository.get_archivo_by_nombre_archivo(filename)
                if not archivo:
                    logger.warning("Archivo no encontrado en la base de datos", extra={"event_filename": filename})

                # Lógica adicional para el archivo encontrado en la base de datos

            except KeyError as e:
                logger.error(f"Estructura de mensaje inválida: {e}", extra={"event_filename": "No filename"})
