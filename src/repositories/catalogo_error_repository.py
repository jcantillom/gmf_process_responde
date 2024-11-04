from typing import Optional
from sqlalchemy.orm import session
from src.models.cgd_error_catalogo import CGDCatalogoErrores
from src.connection.database import DataAccessLayer


class CatalogoErrorRepository:
    """
    Clase que define el repositorio (capa de abstracción a la base de datos) para la entidad 'ErrorCatalogo'.
    """

    def __init__(self, db: session):
        self.db = db

    def get_error_by_code(self, codigo_error: str) -> Optional[CGDCatalogoErrores]:
        """
        Obtiene el error a partir del código de error.
        """

        error = self.db.query(CGDCatalogoErrores).filter(
            CGDCatalogoErrores.codigo_error == codigo_error
        ).first()

        return error
