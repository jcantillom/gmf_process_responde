from sqlalchemy import BOOLEAN, VARCHAR, Column
from sqlalchemy.orm import relationship
from .base import Base


class CGDCatalogoErrores(Base):
    __tablename__: str = "cgd_catalogo_errores"

    codigo_error = Column("codigo_error", VARCHAR(30), primary_key=True)
    descripcion = Column("descripcion", VARCHAR(1000), nullable=False)
    proceso = Column("proceso", VARCHAR(1000), nullable=False)
    aplica_reprogramar = Column("aplica_reprogramar", BOOLEAN, nullable=False)

    rta_pro_archivos = relationship("CGDRtaProArchivos", back_populates="catalogo_error")
    archivos = relationship("CGDArchivo", back_populates="catalogo_error")

    def __repr__(self):
        return (f"<ErrorCatalogo(codigo_error={self.codigo_error}, "
                f"descripcion={self.descripcion}, proceso={self.proceso}, "
                f"aplica_reprogramar={self.aplica_reprogramar})>")
