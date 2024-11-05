from sqlalchemy.orm import Session
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos


class CGDRtaProArchivosRepository:
    """
    Clase para manejar las operaciones de la tabla 'CGD_RTA_PRO_ARCHIVOS'.
    """

    def __init__(self, db: Session):
        self.db = db

    def insert(self, archivo: CGDRtaProArchivos):
        """
        Inserta un registro en la tabla 'CGD_RTA_PRO_ARCHIVOS'.
        """
        self.db.add(archivo)
        self.db.commit()
        self.db.refresh(archivo)
