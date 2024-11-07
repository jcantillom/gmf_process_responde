from sqlalchemy import Column, Numeric, String, CHAR, SmallInteger, Date, TIMESTAMP, DECIMAL, ForeignKey, BigInteger
from .base import Base
from datetime import datetime
from sqlalchemy.orm import relationship


class CGDArchivo(Base):
    __tablename__ = "cgd_archivos"

    id_archivo = Column("id_archivo", BigInteger, primary_key=True, nullable=False)
    nombre_archivo = Column("nombre_archivo", String(100), nullable=False)
    plataforma_origen = Column("plataforma_origen", CHAR(2), nullable=False)
    tipo_archivo = Column("tipo_archivo", CHAR(2), nullable=False)
    consecutivo_plataforma_origen = Column("consecutivo_plataforma_origen", String(4), nullable=False)
    fecha_nombre_archivo = Column("fecha_nombre_archivo", CHAR(8), nullable=False)
    fecha_registro_resumen = Column("fecha_registro_resumen", CHAR(14))
    nro_total_registros = Column("nro_total_registros", Numeric(9))
    nro_registros_error = Column("nro_registros_error", Numeric(9))
    nro_registros_validos = Column("nro_registros_validos", Numeric(9))
    estado = Column("estado", String(50), nullable=False)
    fecha_recepcion = Column("fecha_recepcion", TIMESTAMP, nullable=False)
    fecha_ciclo = Column("fecha_ciclo", Date, nullable=False)
    contador_intentos_cargue = Column("contador_intentos_cargue", SmallInteger, nullable=False)
    contador_intentos_generacion = Column("contador_intentos_generacion", SmallInteger, nullable=False)
    contador_intentos_empaquetado = Column("contador_intentos_empaquetado", SmallInteger, nullable=False)
    acg_fecha_generacion = Column("acg_fecha_generacion", TIMESTAMP)
    acg_consecutivo = Column("acg_consecutivo", Numeric(4))
    acg_nombre_archivo = Column("acg_nombre_archivo", String(100))
    acg_registro_encabezado = Column("acg_registro_encabezado", String(200))
    acg_registro_resumen = Column("acg_registro_resumen", String(200))
    acg_total_tx = Column("acg_total_tx", Numeric(9))
    acg_monto_total_tx = Column("acg_monto_total_tx", Numeric(19, 2))
    acg_total_tx_debito = Column("acg_total_tx_debito", Numeric(9))
    acg_monto_total_tx_debito = Column("acg_monto_total_tx_debito", Numeric(19, 2))
    acg_total_tx_reverso = Column("acg_total_tx_reverso", Numeric(9))
    acg_monto_total_tx_reverso = Column("acg_monto_total_tx_reverso", Numeric(19, 2))
    acg_total_tx_reintegro = Column("acg_total_tx_reintegro", Numeric(9))
    acg_monto_total_tx_reintegro = Column("acg_monto_total_tx_reintegro", Numeric(19, 2))
    anulacion_nombre_archivo = Column("anulacion_nombre_archivo", String(100))
    anulacion_justificacion = Column("anulacion_justificacion", String(4000))
    anulacion_fecha_anulacion = Column("anulacion_fecha_anulacion", TIMESTAMP)
    gaw_rta_trans_estado = Column("gaw_rta_trans_estado", String(50))
    gaw_rta_trans_codigo = Column("gaw_rta_trans_codigo", String(4))
    gaw_rta_trans_detalle = Column("gaw_rta_trans_detalle", String(1000))
    codigo_error = Column("codigo_error", String(30), ForeignKey("cgd_catalogo_errores.codigo_error"))
    detalle_error = Column("detalle_error", String(2000))

    estados = relationship("CGDArchivoEstado", back_populates="archivo")
    catalogo_error = relationship("CGDCatalogoErrores", back_populates="archivos", lazy="joined")

    def __repr__(self):
        return (f"<CGDArchivo(id_archivo={self.id_archivo}, "
                f"nombre_archivo={self.nombre_archivo}, "
                f"estado={self.estado}, "
                f"fecha_recepcion={self.fecha_recepcion}, "
                f"fecha_ciclo={self.fecha_ciclo}, "
                f"contador_intentos_cargue={self.contador_intentos_cargue}, "
                )


class CGDArchivoEstado(Base):
    __tablename__ = "cgd_archivo_estados"

    id_archivo = Column("id_archivo", BigInteger,
                        ForeignKey("cgd_archivos.id_archivo"), primary_key=True)
    estado_inicial = Column("estado_inicial", String(50))
    estado_final = Column("estado_final", String(50), primary_key=True)
    fecha_cambio_estado = Column("fecha_cambio_estado", TIMESTAMP, primary_key=True, nullable=False,
                                 default=datetime.now)

    archivo = relationship("CGDArchivo", back_populates="estados")

    def __repr__(self):
        return (f"<CGDArchivoEstado(id_archivo={self.id_archivo}, "
                f"estado_inicial={self.estado_inicial}, "
                f"estado_final={self.estado_final}, "
                f"fecha_cambio_estado={self.fecha_cambio_estado})>")
