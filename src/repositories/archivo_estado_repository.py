from datetime import datetime
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
        """
        Inserta un nuevo estado de archivo en la tabla 'cgd_archivo_estados'.

        :param id_archivo: ID del archivo.
        :param estado_inicial: Estado inicial del archivo.
        :param estado_final: Estado final del archivo.
        :param fecha_cambio_estado: Fecha del cambio de estado. Por defecto, se usa la fecha actual.
        """
        if fecha_cambio_estado is None:
            fecha_cambio_estado = datetime.now()

        nuevo_estado = CGDArchivoEstado(
            id_archivo=id_archivo,
            estado_inicial=estado_inicial,
            estado_final=estado_final,
            fecha_cambio_estado=fecha_cambio_estado
        )

        # Agrega y confirma el nuevo estado en la base de datos
        self.db.add(nuevo_estado)
        self.db.commit()
        self.db.refresh(nuevo_estado)
