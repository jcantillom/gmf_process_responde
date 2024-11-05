from sqlalchemy.orm import Session
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.config.config import env


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

    def get_pending_files_by_id_archivo(self, id_archivo: int):
        """
        Obtiene los archivos pendientes de procesar por 'ID_ARCHIVO'.
        """
        return self.db.query(CGDRtaProArchivos).filter(
            CGDRtaProArchivos.id_archivo == id_archivo,
            CGDRtaProArchivos.estado == env.CONST_ESTADO_INIT_PENDING
        ).all()

    def update_estado_to_enviado(self, id_archivo: int, nombre_archivo: str):
        """
        Actualiza el estado de un archivo a 'ENVIADO'.
        """
        archivo = self.db.query(CGDRtaProArchivos).filter(
            CGDRtaProArchivos.id_archivo == id_archivo,
            CGDRtaProArchivos.nombre_archivo == nombre_archivo
        ).first()

        if archivo:
            archivo.estado = env.CONST_ESTADO_SEND
            self.db.commit()
