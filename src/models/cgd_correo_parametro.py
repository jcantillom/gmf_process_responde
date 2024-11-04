from sqlalchemy import CHAR, VARCHAR, Column, ForeignKey
from src.connection.database import Base


class CGDCorreosParametros(Base):
    __tablename__ = "cgd_correos_parametros"

    id_plantilla = Column(
        "id_plantilla", CHAR(5),
        ForeignKey("cgd_correos_plantillas.id_plantilla"),
        primary_key=True,
    )
    id_parametro = Column("id_parametro", VARCHAR(30), primary_key=True)
    descripcion = Column("descripcion", VARCHAR(2000), nullable=False)

    def __repr__(self):
        return (f"<CGDCorreosParametros(id_plantilla={self.id_plantilla}, "
                f"id_parametro={self.id_parametro}, "
                f"descripcion={self.descripcion})")
