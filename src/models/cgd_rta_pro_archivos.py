from sqlalchemy import Column, ForeignKey, VARCHAR, BigInteger, SmallInteger, Numeric, ForeignKeyConstraint, Integer
from sqlalchemy.orm import relationship


from .base import Base


class CGDRtaProArchivos(Base):
    __tablename__ = "cgd_rta_pro_archivos"

    id_archivo = Column(BigInteger, primary_key=True, nullable=False)
    id_rta_procesamiento = Column(Integer, primary_key=True)
    nombre_archivo = Column(VARCHAR(100), primary_key=True, nullable=False)
    tipo_archivo_rta = Column(VARCHAR(30), nullable=False)
    estado = Column(VARCHAR(30), nullable=False)
    contador_intentos_cargue = Column(Integer, nullable=False)
    nro_total_registros = Column(Integer)
    nro_registros_error = Column(Integer)
    nro_registros_validos = Column(Integer)
    codigo_error = Column(VARCHAR(30), ForeignKey("cgd_catalogo_errores.codigo_error"))
    detalle_error = Column(VARCHAR(2000))

    catalogo_error = relationship("CGDCatalogoErrores", back_populates="rta_pro_archivos")

    # Define la clave foránea compuesta
    __table_args__ = (
        ForeignKeyConstraint(['id_archivo', 'id_rta_procesamiento'],
                             ['cgd_rta_procesamiento.id_archivo',
                              'cgd_rta_procesamiento.id_rta_procesamiento']),
    )
