from sqlalchemy import CHAR, VARCHAR, BOOLEAN, Column, TIMESTAMP
from .base import Base


class CGDCorreosPlantillas(Base):
    __tablename__ = "cgd_correos_plantillas"

    id_plantilla = Column("id_plantilla", CHAR(5), primary_key=True, nullable=False)
    asunto = Column("asunto", VARCHAR(255), nullable=False)
    cuerpo = Column("cuerpo", VARCHAR, nullable=False)
    remitente = Column("remitente", VARCHAR(100), nullable=False)
    destinatario = Column("destinatario", VARCHAR(1000), nullable=True)
    adjunto = Column("adjunto", BOOLEAN, nullable=False)
    created_at = Column("created_at", TIMESTAMP(timezone=True), nullable=True)
    updated_at = Column("updated_at", TIMESTAMP(timezone=True), nullable=True)

    def __repr__(self):
        return (f"<CGDCorreosPlantillas(id_plantilla={self.id_plantilla}, "
                f"asunto={self.asunto}, cuerpo={self.cuerpo}, "
                f"remitente={self.remitente}, destinatario={self.destinatario}, "
                f"adjunto={self.adjunto}, created_at={self.created_at}, "
                f"updated_at={self.updated_at})>")
