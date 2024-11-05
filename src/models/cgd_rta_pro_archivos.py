from sqlalchemy import Numeric, SMALLINT, VARCHAR, Column, ForeignKey
from src.connection.database import Base


class CGDRtaProArchivos(Base):
    """
    Clase que define el modelo SQLAlchemy para la tabla 'CGD_RTA_PRO_ARCHIVOS'.
    """

    __tablename__ = "cgd_rta_pro_archivos"

    id_archivo = Column(
        "id_archivo",
        Numeric(16),
        ForeignKey("cgd_rta_procesamiento.id_archivo"),
        primary_key=True,
    )
    id_rta_procesamiento = Column("id_rta_procesamiento", Numeric(2),
                                  ForeignKey("cgd_rta_procesamiento.id_rta_procesamiento"), primary_key=True)

    nombre_archivo = Column("nombre_archivo", VARCHAR(100), primary_key=True)
    tipo_archivo_rta = Column("tipo_archivo_rta", VARCHAR(30), nullable=False)
    estado = Column("estado", VARCHAR(30), nullable=False)
    contador_intentos_cargue = Column("contador_intentos_cargue", SMALLINT, nullable=False)
    nro_total_registros = Column("nro_total_registros", Numeric(9))
    nro_registros_error = Column("nro_registros_error", Numeric(9))
    nro_registros_validos = Column("nro_registros_validos", Numeric(9))
    codigo_error = Column(
        "codigo_error", VARCHAR(30), ForeignKey("cgd_catalogo_errores.codigo_error"))
    detalle_error = Column("detalle_error", VARCHAR(2000))
