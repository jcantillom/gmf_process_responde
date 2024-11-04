from sqlalchemy import CHAR, VARCHAR, Column, ForeignKey, Numeric, TIMESTAMP, Integer, Sequence
from src.connection.database import Base


class CGDRtaProcesamiento(Base):
    __tablename__ = "cgd_rta_procesamiento"

    id_archivo = Column("id_archivo", Numeric(16), primary_key=True)
    id_rta_procesamiento = Column("id_rta_procesamiento", Integer, primary_key=True, autoincrement=True)
    nombre_archivo_zip = Column("nombre_archivo_zip", VARCHAR(100), nullable=False)
    tipo_respuesta = Column("tipo_respuesta", CHAR(2), nullable=False)
    fecha_recepcion = Column("fecha_recepcion", TIMESTAMP, nullable=False)
    estado = Column("estado", VARCHAR(50), nullable=False)
    contador_intentos_cargue = Column("contador_intentos_cargue", Numeric(5), nullable=False)
    codigo_error = Column(
        "codigo_error",
        VARCHAR(30),
        ForeignKey("cgd_catalogo_errores.codigo_error"),
    )
    detalle_error = Column("detalle_error", VARCHAR(2000))

    def __repr__(self):
        return (f"<CGDRtaProcesamiento(id_archivo={self.id_archivo}, "
                f"id_rta_procesamiento={self.id_rta_procesamiento}, "
                f"nombre_archivo_zip={self.nombre_archivo_zip}, "
                f"tipo_respuesta={self.tipo_respuesta}, "
                f"fecha_recepcion={self.fecha_recepcion}, "
                f"estado={self.estado}, "
                f"contador_intentos_cargue={self.contador_intentos_cargue}, "
                f"codigo_error={self.codigo_error}, "
                f"detalle_error={self.detalle_error})>")
