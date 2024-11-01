from typing import Type, Optional
from sqlalchemy.orm import Session
from src.models.archivo_models import CGDArchivo, CGDArchivoEstado


class ArchivoRepository:
    """
    Clase que define el repositorio (capa de abstracción a la base de datos) para la entidad 'Archivo'.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_archivo_by_nombre_archivo(self, nombre_archivo: str) -> Optional[CGDArchivo]:
        """
        Obtiene un archivo por su nombre.

        :param nombre_archivo: Nombre del archivo a buscar.
        :return: Instancia de CGDArchivo o None si no se encuentra.
        """
        return self.db.query(CGDArchivo).filter(
            CGDArchivo.acg_nombre_archivo == nombre_archivo
        ).first()
