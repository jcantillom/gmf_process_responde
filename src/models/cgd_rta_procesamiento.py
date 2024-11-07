# src/models/cgd_rta_procesamiento.py
from sqlalchemy import CHAR, VARCHAR, Column, ForeignKey, Numeric, TIMESTAMP, Integer, PrimaryKeyConstraint, BigInteger
from .base import Base


class CGDRtaProcesamiento(Base):
    __tablename__ = "cgd_rta_procesamiento"

    id_archivo = Column(BigInteger, nullable=False)
    id_rta_procesamiento = Column(Integer, nullable=False, autoincrement=True)
    nombre_archivo_zip = Column(VARCHAR(100), nullable=False)
    tipo_respuesta = Column(VARCHAR(2), nullable=False)
    fecha_recepcion = Column(TIMESTAMP, nullable=False)
    estado = Column(VARCHAR(50), nullable=False)
    contador_intentos_cargue = Column(Integer, nullable=False)
    codigo_error = Column(VARCHAR(30), ForeignKey("cgd_catalogo_errores.codigo_error"))
    detalle_error = Column(VARCHAR(2000))

    # Define la clave primaria compuesta
    __table_args__ = (
        PrimaryKeyConstraint('id_archivo', 'id_rta_procesamiento'),
    )
