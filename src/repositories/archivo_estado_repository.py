from datetime import datetime

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from src.models.cgd_archivo import CGDArchivoEstado
from src.utils.logger_utils import get_logger
from src.config.config import env

logger = get_logger(env.DEBUG_MODE)


class ArchivoEstadoRepository:
    def __init__(self, db: Session):
        self.db = db

    def insert_estado_archivo(self, id_archivo: int, estado_inicial: str, estado_final: str,
                              fecha_cambio_estado: datetime = None) -> None:
        if fecha_cambio_estado is None:
            fecha_cambio_estado = datetime.now()

        nuevo_estado = CGDArchivoEstado(
            id_archivo=id_archivo,
            estado_inicial=estado_inicial,
            estado_final=estado_final,
            fecha_cambio_estado=fecha_cambio_estado
        )

        try:
            self.db.add(nuevo_estado)
            self.db.commit()
            self.db.refresh(nuevo_estado)
        except IntegrityError as e:
            self.db.rollback()
            if 'duplicate key value violates unique constraint' in str(e.orig):
                conflict_detail = str(e.orig).split('DETAIL: ')[-1]
                logger.error(f"Error de clave duplicada: {conflict_detail}")
                raise ValueError(f"Error de clave duplicada: {conflict_detail}")
            else:
                logger.error(f"Error en la base de datos: {e}")
                raise ValueError(f"Error en la base de datos: {e}")
