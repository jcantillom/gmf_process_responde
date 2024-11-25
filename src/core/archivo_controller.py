from src.services.archivo_service import ArchivoService
from sqlalchemy.orm import Session


def process_sqs_message(event, db: Session):
    """
    Controlador para procesar mensajes de SQS y llamar al servicio de negocio.
    """
    archivo_service = ArchivoService(db)
    archivo_service.validar_y_procesar_archivo(event)
