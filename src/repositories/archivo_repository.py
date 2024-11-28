from typing import Type, Optional, Any
from sqlalchemy.orm import Session

from src.core.custom_error import CustomFunctionError
from src.config.config import env
from src.models.cgd_archivo import CGDArchivo, CGDArchivoEstado


class ArchivoRepository:
    """
    Clase que define el repositorio (capa de abstracción a la base de datos) para la entidad 'Archivo'.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_archivo_by_nombre_archivo(self, nombre_archivo: str) -> Type[CGDArchivo]:
        """
        Obtiene un archivo por su nombre.

        :param nombre_archivo: Nombre del archivo a buscar.
        :return: Instancia de CGDArchivo o None si no se encuentra.
        """
        try:
            result = self.db.query(CGDArchivo).filter(
                CGDArchivo.acg_nombre_archivo == str(nombre_archivo)
            ).first()

            if result:
                return result
        except Exception as e:
            print(f"Error al buscar archivo por nombre: {e}")

    def check_file_exists(self, nombre_archivo: str) -> bool:
        """
        Verifica si un archivo existe en la base de datos.

        :param nombre_archivo: Nombre del archivo a buscar.
        :return: True si existe, False si no.
        """
        try:
            archivo = self.get_archivo_by_nombre_archivo(nombre_archivo)
            return archivo is not None
        except Exception as e:
            raise CustomFunctionError(
                code=env.CONST_COD_ERROR_NOT_EXISTS_FILE,
                error_details=f"El archivo {nombre_archivo} no existe en la base de datos.",
                is_technical_error=False,
            )

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

    def insert_code_error(
            self,
            id_archivo: int,
            code_error: str,
            detail_error: str,
    ) -> None:
        """
        Inserta un error en la tabla CGD_ARCHIVO.

        :param id_archivo: ID del archivo.
        :param code_error: Código de error.
        :param detail_error: Detalle del error.
        """
        archivo = self.db.query(CGDArchivo).filter(CGDArchivo.id_archivo == id_archivo).first()
        archivo.codigo_error = code_error
        archivo.detalle_error = detail_error
        self.db.commit()
