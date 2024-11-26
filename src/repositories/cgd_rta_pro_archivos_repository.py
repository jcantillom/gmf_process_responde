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

    def get_files_loaded_for_response(self, id_archivo: int, id_rta_procesamiento: int) -> list:
        """
        Válida si ya existen archivos cargados para un ID_ARCHIVO y un ID_RTA_PROCESAMIENTO específicos.

        :param id_archivo: ID del archivo (file_id).
        :param id_rta_procesamiento: ID de la respuesta de procesamiento (response_processing_id).
        :return: True si existen archivos cargados, False en caso contrario.
        """
        # Consulta para verificar si existen registros en la tabla CGD_RTA_PRO_ARCHIVOS
        result = self.db.query(CGDRtaProArchivos).filter(
            CGDRtaProArchivos.id_archivo == id_archivo,
            CGDRtaProArchivos.id_rta_procesamiento == id_rta_procesamiento
        ).all()

        return result
