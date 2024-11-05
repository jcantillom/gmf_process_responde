from typing import Optional, Type
from sqlalchemy.orm import Session
from src.models.cgd_correo_parametro import CGDCorreosParametros


class CorreoParametroRepository:
    """
    Clase que define el repositorio (capa de abstracción a la base de datos) para la entidad 'CorreoParametro'.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_parameters_by_template(self, id_plantilla: str) -> list[Type[CGDCorreosParametros]]:
        """
        Obtiene los parámetros de una plantilla a partir de su identificador.
        """

        parameters = self.db.query(CGDCorreosParametros).filter(
            CGDCorreosParametros.id_plantilla == id_plantilla
        ).all()

        return parameters
