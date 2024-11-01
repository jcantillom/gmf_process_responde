from pydantic import BaseModel, Field, condecimal, constr
from typing import Optional
from datetime import datetime


class CGDArchivoSchema(BaseModel):
    id_archivo: int
    nombre_archivo: constr(max_length=100)
    plataforma_origen: constr(max_length=2)
    tipo_archivo: constr(max_length=2)
    consecutivo_plataforma_origen: int
    fecha_nombre_archivo: constr(max_length=8)
    fecha_registro_resumen: Optional[constr(max_length=14)]
    nro_total_registros: Optional[int]
    nro_registros_error: Optional[int]
    nro_registros_validos: Optional[int]
    estado: constr(max_length=50)
    fecha_recepcion: datetime
    fecha_ciclo: datetime
    contador_intentos_cargue: int
    contador_intentos_generacion: int
    contador_intentos_empaquetado: int
    acg_fecha_generacion: Optional[datetime]
    acg_consecutivo: Optional[int]
    acg_nombre_archivo: Optional[constr(max_length=100)]
    acg_registro_encabezado: Optional[constr(max_length=200)]
    acg_registro_resumen: Optional[constr(max_length=200)]
    acg_total_tx: Optional[int]
    acg_monto_total_tx: Optional[condecimal(max_digits=19, decimal_places=2)]
    acg_total_tx_debito: Optional[int]
    acg_monto_total_tx_debito: Optional[condecimal(max_digits=19, decimal_places=2)]
    acg_total_tx_reverso: Optional[int]
    acg_monto_total_tx_reverso: Optional[condecimal(max_digits=19, decimal_places=2)]
    acg_total_tx_reintegro: Optional[int]
    acg_monto_total_tx_reintegro: Optional[condecimal(max_digits=19, decimal_places=2)]
    anulacion_nombre_archivo: Optional[constr(max_length=100)]
    anulacion_justificacion: Optional[constr(max_length=4000)]
    anulacion_fecha_anulacion: Optional[datetime]
    gaw_rta_trans_estado: Optional[constr(max_length=50)]
    gaw_rta_trans_codigo: Optional[constr(max_length=4)]
    gaw_rta_trans_detalle: Optional[constr(max_length=1000)]
    id_prc_genera_consol: Optional[int]
    codigo_error: Optional[constr(max_length=30)]
    detalle_error: Optional[constr(max_length=2000)]

    class Config:
        from_attributes = True


class CGDArchivoEstadoSchema(BaseModel):
    id_archivo: int
    estado_inicial: Optional[constr(max_length=50)]
    estado_final: constr(max_length=50)
    fecha_cambio_estado: datetime = Field(default_factory=datetime.now)

    class Config:
        from_attributes = True
