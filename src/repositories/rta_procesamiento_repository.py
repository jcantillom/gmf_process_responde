from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from .archivo_repository import ArchivoRepository
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from src.core.validator import ArchivoValidator
from src.config.config import env


class ProcessingResponseNotFoundError(Exception):
    pass


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
            id_rta_procesamiento: int,
            nombre_archivo_zip: str,
            tipo_respuesta: str,
            estado: str,
            contador_intentos_cargue: int,
            codigo_error: str = None,
            detalle_error: str = None) -> None:
        """
        Inserta una nueva respuesta de procesamiento en la tabla 'cgd_rta_procesamiento'.

        :param id_archivo: ID del archivo.
        :param id_rta_procesamiento: ID de la respuesta de procesamiento.
        :param nombre_archivo_zip: Nombre del archivo ZIP.
        :param tipo_respuesta: Tipo de respuesta.
        :param estado: Estado de la respuesta.
        :param contador_intentos_cargue: Contador de intentos de cargue.
        :param codigo_error: Código de error. Por defecto, es None.
        :param detalle_error: Detalle del error. Por defecto, es None.

        """
        # Definir la zona horaria de Colombia (UTC-5)
        colombia_tz = timezone(timedelta(hours=-5))
        fecha_recepcion = datetime.now(colombia_tz)

        nueva_rta_procesamiento = CGDRtaProcesamiento(
            id_archivo=id_archivo,
            id_rta_procesamiento=id_rta_procesamiento,
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
            raise ProcessingResponseNotFoundError(
                f"No se encontró una respuesta de procesamiento para el archivo con ID {id_archivo}")

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

    def get_last_rta_procesamiento_without_archivos(self, id_archivo: int, nombre_archivo_zip: str) -> int:
        """
        Obtiene el id_rta_procesamiento de la tabla CGD_RTA_PROCESAMIENTO basado en
        el id_archivo y nombre_archivo_zip Y QUE NO TENGA REGISTRO EN CGD_RTA_PRO_ARCHIVOS.
        """

        result = self.db.query(CGDRtaProcesamiento.id_rta_procesamiento).join(
            CGDRtaProArchivos,
            (CGDRtaProcesamiento.id_archivo == CGDRtaProArchivos.id_archivo) & (
                    CGDRtaProcesamiento.id_rta_procesamiento == CGDRtaProArchivos.id_rta_procesamiento),
            isouter=True
        ).filter(
            CGDRtaProcesamiento.id_archivo == id_archivo,
            CGDRtaProcesamiento.nombre_archivo_zip == nombre_archivo_zip).filter(
            CGDRtaProArchivos.id_rta_procesamiento == None).order_by(
            CGDRtaProcesamiento.id_rta_procesamiento.desc()
        ).first()

        return result.id_rta_procesamiento if result else None

    def get_id_rta_procesamiento_by_id_archivo(self, id_archivo: int, nombre_archivo_zip: str) -> int:
        """
        Obtiene el id_rta_procesamiento de la tabla CGD_RTA_PROCESAMIENTO basado en
        el id_archivo y nombre_archivo_zip.
        """

        result = self.db.query(CGDRtaProcesamiento.id_rta_procesamiento).filter(
            CGDRtaProcesamiento.id_archivo == id_archivo,
            CGDRtaProcesamiento.nombre_archivo_zip == nombre_archivo_zip).order_by(
            CGDRtaProcesamiento.id_rta_procesamiento.desc()).first()

        return result.id_rta_procesamiento if result else None

    def get_last_rta_procesamiento(self, id_archivo: int):
        """
        Obtiene el último id_rta_procesamiento para un id_archivo dado.

        Args:
            id_archivo (int): El id_archivo para el que obtener el último id_rta_procesamiento.

        Returns:
            CGDRtaProcesamiento: El último registro de id_rta_procesamiento para el id_archivo dado.
        """
        # Realizamos una consulta para obtener el último id_rta_procesamiento para el id_archivo
        last_rta_procesamiento = self.db.query(CGDRtaProcesamiento).filter(
            CGDRtaProcesamiento.id_archivo == id_archivo
        ).order_by(CGDRtaProcesamiento.id_rta_procesamiento.desc()).first()

        return last_rta_procesamiento

    # actualizar el contador de intentos de cargue
    def update_contador_intentos_cargue(self, id_archivo: int, contador_intentos_cargue: int):
        """
        Actualiza el contador de intentos de cargue de una respuesta de procesamiento.

        """
        last_entry = self.get_last_rta_procesamiento(id_archivo)

        last_entry.contador_intentos_cargue = contador_intentos_cargue

        # Confirmamos los cambios en la base de datos
        self.db.commit()
        self.db.refresh(last_entry)
