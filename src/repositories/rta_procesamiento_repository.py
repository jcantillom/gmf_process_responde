import sys

from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento
from .archivo_repository import ArchivoRepository
from datetime import datetime
from sqlalchemy.orm import Session
from src.utils.validator_utils import ArchivoValidator
from src.config.config import env


class RtaProcesamientoRepository:
    def __init__(self, db: Session):
        self.db = db
        self.archivo_repository = ArchivoRepository(db)
        self.archivo_validator = ArchivoValidator()

    def get_last_contador_intentos_cargue(self, id_archivo: int) -> int:
        """
        Obtiene el último contador de intentos de cargue de una respuesta de procesamiento.

        :param id_archivo: ID del archivo.
        :return: Último contador de intentos de cargue.
        """
        # Obtiene la última respuesta de procesamiento
        last_entry = self.db.query(CGDRtaProcesamiento) \
            .filter(CGDRtaProcesamiento.id_archivo == id_archivo) \
            .order_by(CGDRtaProcesamiento.id_rta_procesamiento.desc()) \
            .first()

        return last_entry.contador_intentos_cargue if last_entry else 0

    def insert_rta_procesamiento(
            self,
            id_archivo: int,
            nombre_archivo_zip: str,
            tipo_respuesta: str,
            estado: str,
            contador_intentos_cargue: int,
            codigo_error: str = None,
            detalle_error: str = None) -> None:
        """
        Inserta una nueva respuesta de procesamiento en la tabla 'cgd_rta_procesamiento'.

        :param id_archivo: ID del archivo.
        :param nombre_archivo_zip: Nombre del archivo ZIP.
        :param tipo_respuesta: Tipo de respuesta.
        :param estado: Estado de la respuesta.
        :param contador_intentos_cargue: Contador de intentos de cargue.
        :param codigo_error: Código de error. Por defecto, es None.
        :param detalle_error: Detalle del error. Por defecto, es None.
        """
        fecha_recepcion = datetime.now()

        nueva_rta_procesamiento = CGDRtaProcesamiento(
            id_archivo=id_archivo,
            nombre_archivo_zip=nombre_archivo_zip,
            tipo_respuesta=tipo_respuesta,
            fecha_recepcion=fecha_recepcion,
            estado=estado,
            contador_intentos_cargue=contador_intentos_cargue,
            codigo_error=codigo_error,
            detalle_error=detalle_error
        )

        try:
            # Agrega y confirma la nueva respuesta de procesamiento en la base de datos
            self.db.add(nueva_rta_procesamiento)
            self.db.commit()
            self.db.refresh(nueva_rta_procesamiento)

        except Exception as e:
            # Revertir los cambios para mantener la consistencia en la base de datos
            self.db.rollback()
            raise e

        # TODO: validar si es necesario actualizar el estado del archivo
        # Actualizar el estado del archivo
        # nombre_archivo = self.archivo_validator.build_acg_nombre_archivo(nombre_archivo_zip)
        # self.archivo_repository.update_estado_archivo(
        #     nombre_archivo,
        #     estado,
        #     contador_intentos_cargue
        # )

    def update_state_rta_procesamiento(
            self,
            id_archivo: int,
            estado: str
    ) -> None:
        """
        Actualiza el estado de una respuesta de procesamiento.

        :param id_archivo: ID del archivo.
        :param estado: Estado de la respuesta.
        """
        # Obtiene la última respuesta de procesamiento
        last_entry = self.db.query(CGDRtaProcesamiento) \
            .filter(CGDRtaProcesamiento.id_archivo == id_archivo) \
            .order_by(CGDRtaProcesamiento.id_rta_procesamiento.desc()) \
            .first()

        if last_entry:
            last_entry.estado = estado
            self.db.commit()
        else:
            sys.exit(1)

    def get_tipo_respuesta(self, id_archivo: int) -> str:
        """
        Obtiene el tipo de respuesta de una respuesta de procesamiento.

        :param id_archivo: ID del archivo.
        :return: Tipo de respuesta.
        """
        # Obtiene la última respuesta de procesamiento
        last_entry = self.db.query(CGDRtaProcesamiento) \
            .filter(CGDRtaProcesamiento.id_archivo == id_archivo) \
            .order_by(CGDRtaProcesamiento.id_rta_procesamiento.desc()) \
            .first()

        return last_entry.tipo_respuesta if last_entry else None

    def is_estado_enviado(self, id_archivo: int, nombre_archivo_zip: str) -> bool:
        """
        Verifica si el estado de la respuesta de procesamiento es 'ENVIADO'.

        :param id_archivo: ID del archivo.
        :param nombre_archivo_zip: Nombre del archivo ZIP.
        :return: True si el estado es 'ENVIADO', False en caso contrario.
        """
        # Obtiene la última respuesta de procesamiento
        last_entry = self.db.query(CGDRtaProcesamiento) \
            .filter(CGDRtaProcesamiento.id_archivo == id_archivo,
                    CGDRtaProcesamiento.nombre_archivo_zip == nombre_archivo_zip) \
            .order_by(CGDRtaProcesamiento.id_rta_procesamiento.desc()) \
            .first()
        return last_entry.estado == env.CONST_ESTADO_SEND if last_entry else False

    def get_id_rta_procesamiento_by_id_archivo(self, id_archivo: int, nombre_archivo_zip: str) -> int:
        """
        Obtiene el id_rta_procesamiento de la tabla CGD_RTA_PROCESAMIENTO basado en
        el id_archivo y nombre_archivo_zip.

        Returns:
            int: El id_rta_procesamiento si se encuentra, None en caso contrario.
        """

        result = self.db.query(CGDRtaProcesamiento.id_rta_procesamiento).filter(
            CGDRtaProcesamiento.id_archivo == id_archivo,
            CGDRtaProcesamiento.nombre_archivo_zip == nombre_archivo_zip
        ).first()

        return result.id_rta_procesamiento if result else None
