from typing import Type, Optional
from sqlalchemy.orm import Session
from src.models.cgd_archivo import CGDArchivo, CGDArchivoEstado


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
            CGDArchivo.acg_nombre_archivo == str(nombre_archivo)
        ).first()

    def check_file_exists(self, nombre_archivo: str) -> bool:
        """
        Verifica si un archivo existe en la base de datos.

        :param nombre_archivo: Nombre del archivo a buscar.
        :return: True si existe, False si no.
        """
        archivo = self.get_archivo_by_nombre_archivo(nombre_archivo)
        return archivo is not None

    def update_estado_archivo(
            self,
            nombre_archivo: str,
            estado: str,
            contador_intentos_cargue: int) -> None:
        """
        Actualiza el estado de un archivo en la base de datos.


        :param nombre_archivo: Nombre del archivo a actualizar.
        :param estado: Nuevo estado del archivo.
        :param contador_intentos_cargue: Contador de intentos de cargue.

        """
        archivo = self.get_archivo_by_nombre_archivo(nombre_archivo)
        archivo.estado = estado
        archivo.contador_intentos_cargue = contador_intentos_cargue
        self.db.commit()

    def check_special_file_exists(self, acg_nombre_archivo: str, tipo_archivo: str) -> bool:
        """
        Verifica si un archivo especial existe en la base de datos.

        :param acg_nombre_archivo: Nombre del archivo a buscar.
        :param tipo_archivo: Tipo de archivo a buscar.
        :return: True si existe, False si no.
        """
        archivo = self.db.query(CGDArchivo).filter(
            CGDArchivo.acg_nombre_archivo == str(acg_nombre_archivo),
            CGDArchivo.tipo_archivo == str(tipo_archivo)
        ).first()

        return archivo is not None

    def insert_archivo(self, archivo: CGDArchivo) -> None:
        """
        Inserta un archivo en la base de datos.

        :param archivo: Instancia de CGDArchivo a insertar.
        """
        self.db.add(archivo)
        self.db.commit()
