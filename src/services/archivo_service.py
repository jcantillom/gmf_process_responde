import json
from src.repositories.archivo_repository import ArchivoRepository
from src.utils.s3_utils import check_file_exists_in_s3, extract_filename_from_body, extract_bucket_from_body
from src.logs import logger as log
from sqlalchemy.orm import Session


class ArchivoService:
    def __init__(self, db: Session):
        self.repository = ArchivoRepository(db)

    def validar_y_procesar_archivo(self, event):
        """
        Válida el mensaje de SQS y verifica la existencia del archivo en S3.
        """
        for record in event.get("Records", []):
            try:
                # Extrae y valida la información del archivo desde el mensaje
                body = record.get("body", "{}")
                filename = extract_filename_from_body(body)
                bucket = extract_bucket_from_body(body)

                # Log con el nombre del archivo
                log.logger.info("Procesando archivo", extra={"event_filename": filename})

                # Verificar que el archivo existe en S3
                if not check_file_exists_in_s3(bucket, filename):
                    log.logger.error("El archivo no existe en el bucket", extra={"event_filename": filename})
                    # Aquí se debería eliminar el mensaje de la cola si no es válido.
                    continue

                # Busca el archivo en la base de datos (si aplica)
                archivo = self.repository.get_archivo_by_nombre_archivo(filename)
                if not archivo:
                    log.logger.warning("Archivo no encontrado en la base de datos", extra={"event_filename": filename})

                # Lógica adicional para el archivo encontrado en la base de datos

            except KeyError as e:
                log.logger.error(f"Estructura de mensaje inválida: {e}", extra={"event_filename": "No filename"})
