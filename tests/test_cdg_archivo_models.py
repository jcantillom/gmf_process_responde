from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models.cgd_archivo import CGDArchivo, CGDArchivoEstado
from src.models.base import Base
from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.models.cgd_error_catalogo import CGDCatalogoErrores
from src.models.cgd_archivo import CGDArchivo


@pytest.fixture(scope="module")
def engine():
    engine = create_engine("sqlite:///:memory:", echo=True)
    Base.metadata.create_all(engine)
    TestingSessionLocal = sessionmaker(bind=engine)

    # crear un nuevo session
    session = TestingSessionLocal()
    yield session

    # cerrar la session
    session.close()


@pytest.mark.parametrize(
    "nombre_archivo, plataforma_origen, tipo_archivo, estado, fecha_recepcion, fecha_ciclo, contador_intentos_cargue",
    [
        ("archivo1.txt", "PL", "TY", "Nuevo", datetime.now(), datetime.now(), 0),
        ("archivo2.txt", "AP", "TX", "En Proceso", datetime.now(), datetime.now(), 1)
    ])
@pytest.mark.parametrize(
    "nombre_archivo, plataforma_origen, tipo_archivo, estado, fecha_recepcion, fecha_ciclo, contador_intentos_cargue",
    [
        ("archivo1.txt", "PL", "TY", "Nuevo", datetime.now(), datetime.now(), 0),
        ("archivo2.txt", "AP", "TX", "En Proceso", datetime.now(), datetime.now(), 1)
    ])
def test_create_cgd_archivo(test_db, nombre_archivo, plataforma_origen, tipo_archivo, estado, fecha_recepcion,
                            fecha_ciclo, contador_intentos_cargue):
    # Crear una instancia de CGDArchivo
    nuevo_archivo = CGDArchivo(
        nombre_archivo=nombre_archivo,
        plataforma_origen=plataforma_origen,
        tipo_archivo=tipo_archivo,
        consecutivo_plataforma_origen="0001",
        fecha_nombre_archivo="20210101",
        fecha_registro_resumen="20210101",
        nro_total_registros=100,
        nro_registros_error=5,
        nro_registros_validos=95,
        estado=estado,
        fecha_recepcion=fecha_recepcion,
        fecha_ciclo=fecha_ciclo,
        contador_intentos_cargue=contador_intentos_cargue,
        contador_intentos_generacion=0,
        contador_intentos_empaquetado=0,
        acg_fecha_generacion=None,
        acg_consecutivo=None,
        acg_nombre_archivo=None,
        acg_registro_encabezado=None,
        acg_registro_resumen=None,
        acg_total_tx=0,
        acg_monto_total_tx=0.0,
        acg_total_tx_debito=0,
        acg_monto_total_tx_debito=0.0,
        acg_total_tx_reverso=0,
        acg_monto_total_tx_reverso=0.0,
        acg_total_tx_reintegro=0,
        acg_monto_total_tx_reintegro=0.0,
        anulacion_nombre_archivo=None,
        anulacion_justificacion=None,
        anulacion_fecha_anulacion=None,
        gaw_rta_trans_estado=None,
        gaw_rta_trans_codigo=None,
        gaw_rta_trans_detalle=None,
        codigo_error=None,
        detalle_error=None
    )

    # Añadir el nuevo archivo a la sesión de prueba
    test_db.add(nuevo_archivo)
    test_db.commit()

    # Verificar que el archivo ha sido añadido correctamente
    assert nuevo_archivo.id_archivo is not None
    assert nuevo_archivo.nombre_archivo == nombre_archivo
    assert nuevo_archivo.estado == estado
