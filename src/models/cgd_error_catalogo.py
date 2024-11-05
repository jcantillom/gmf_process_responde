from sqlalchemy import BOOLEAN, VARCHAR, Column
from src.connection.database import Base
from sqlalchemy.orm import relationship


class CGDCatalogoErrores(Base):
    __tablename__: str = "cgd_catalogo_errores"

    codigo_error = Column("codigo_error", VARCHAR(30), primary_key=True)
    descripcion = Column("descripcion", VARCHAR(1000), nullable=False)
    proceso = Column("proceso", VARCHAR(1000), nullable=False)
    aplica_reprogramar = Column("aplica_reprogramar", BOOLEAN, nullable=False)

    # Relaciones
    rta_pro_archivos = relationship("CGDRtaProArchivos", back_populates="catalogo_errores")

    def __repr__(self):
        return (f"<ErrorCatalogo(codigo_error={self.codigo_error}, "
                f"descripcion={self.descripcion}, proceso={self.proceso}, "
                f"aplica_reprogramar={self.aplica_reprogramar})>")
