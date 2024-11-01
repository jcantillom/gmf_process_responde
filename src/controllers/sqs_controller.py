from src.logs import logger as log
from src.services.archivo_service import ArchivoService
from sqlalchemy.orm import Session
from src.connection.database import DataAccessLayer
from src.schemas.archivo_schema import CGDArchivoSchema


def process_sqs_message(event, db: Session):
    """
    Controlador para procesar mensajes de SQS y llamar al servicio de negocio.
    """
    archivo_service = ArchivoService(db)
    archivo_service.validar_y_procesar_archivo(event)
