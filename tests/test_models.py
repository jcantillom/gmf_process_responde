import unittest
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.connection.database import Base
from src.models.cgd_archivo import CGDArchivo, CGDArchivoEstado
from src.models.cgd_error_catalogo import CGDCatalogoErrores
from src.models.cgd_correo_parametro import CGDCorreosParametros
from src.models.cgd_correos_plantilla import CGDCorreosPlantillas
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento


class TestCGDArchivo(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Agregar un error a la tabla de catálogo de errores para poder hacer la referencia
        error = CGDCatalogoErrores(
            codigo_error='test',
            descripcion='Test Error',
            proceso='Test Proceso',
            aplica_reprogramar=True
        )
        self.session.add(error)
        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_create_cgd_archivo(self):
        cgd_archivo = CGDArchivo(
            id_archivo=1,
            nombre_archivo='archivo.txt',
            plataforma_origen='01',
            tipo_archivo='01',
            consecutivo_plataforma_origen=1,
            fecha_nombre_archivo='20210101',
            fecha_registro_resumen='20210101120000',
            nro_total_registros=100,
            nro_registros_error=0,
            nro_registros_validos=100,
            estado='CARGADO',
            fecha_recepcion=datetime.now(),
            fecha_ciclo=datetime.now(),
            contador_intentos_cargue=0,
            contador_intentos_generacion=0,
            contador_intentos_empaquetado=0,
            acg_fecha_generacion=datetime.now(),
            acg_consecutivo=1,
            acg_nombre_archivo='archivo.txt',
            acg_registro_encabezado='encabezado',
            acg_registro_resumen='resumen',
            acg_total_tx=100,
            acg_monto_total_tx=100.00,
            acg_total_tx_debito=0,
            acg_monto_total_tx_debito=0.00,
            acg_total_tx_reverso=0,
            acg_monto_total_tx_reverso=0.00,
            acg_total_tx_reintegro=0,
            acg_monto_total_tx_reintegro=0.00,
            anulacion_nombre_archivo="test",
            anulacion_justificacion="test",
            anulacion_fecha_anulacion=datetime.now(),
            gaw_rta_trans_estado="test",
            gaw_rta_trans_codigo="test",
            gaw_rta_trans_detalle="test",
            codigo_error="test",  # Esto debe coincidir con el error que insertaste
            detalle_error="test"
        )
        self.session.add(cgd_archivo)
        self.session.commit()
        self.assertEqual(self.session.query(CGDArchivo).count(), 1)


class TestCGDArchivoEstado(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Agregar un archivo a la tabla cgd_archivos
        archivo = CGDArchivo(
            id_archivo=1,
            nombre_archivo='archivo.txt',
            plataforma_origen='01',
            tipo_archivo='01',
            consecutivo_plataforma_origen=1,
            fecha_nombre_archivo='20210101',
            fecha_registro_resumen='20210101120000',
            nro_total_registros=100,
            nro_registros_error=0,
            nro_registros_validos=100,
            estado='CARGADO',
            fecha_recepcion=datetime.now(),
            fecha_ciclo=datetime.now(),
            contador_intentos_cargue=0,
            contador_intentos_generacion=0,
            contador_intentos_empaquetado=0,
            acg_fecha_generacion=datetime.now(),
            acg_consecutivo=1,
            acg_nombre_archivo='archivo.txt',
            acg_registro_encabezado='encabezado',
            acg_registro_resumen='resumen',
            acg_total_tx=100,
            acg_monto_total_tx=100.00,
            acg_total_tx_debito=0,
            acg_monto_total_tx_debito=0.00,
            acg_total_tx_reverso=0,
            acg_monto_total_tx_reverso=0.00,
            acg_total_tx_reintegro=0,
            acg_monto_total_tx_reintegro=0.00,
            anulacion_nombre_archivo="test",
            anulacion_justificacion="test",
            anulacion_fecha_anulacion=datetime.now(),
            gaw_rta_trans_estado="test",
            gaw_rta_trans_codigo="test",
            gaw_rta_trans_detalle="test",
            codigo_error="test",
            detalle_error="test"
        )
        self.session.add(archivo)
        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_create_cgd_archivo_estado(self):
        estado_archivo = CGDArchivoEstado(
            id_archivo=1,
            estado_inicial='CARGADO',
            estado_final='PROCESANDO',
            fecha_cambio_estado=datetime.now()
        )
        self.session.add(estado_archivo)
        self.session.commit()
        self.assertEqual(self.session.query(CGDArchivoEstado).count(), 1)


class TestCGDCorreosParametros(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        # Agregar una plantilla de correo a la tabla cgd_correos_plantillas
        plantilla = CGDCorreosPlantillas(
            id_plantilla='test01',
            asunto='Asunto de prueba',
            cuerpo='Cuerpo de prueba',
            remitente='remitente@test.com',
            destinatario='destinatario@test.com',
            adjunto=False,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        self.session.add(plantilla)
        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_create_cgd_correos_parametros(self):
        parametro = CGDCorreosParametros(
            id_plantilla='test01',  # Debe coincidir con la plantilla creada
            id_parametro='parametro01',
            descripcion='Descripción del parámetro de correo'
        )
        self.session.add(parametro)
        self.session.commit()
        self.assertEqual(self.session.query(CGDCorreosParametros).count(), 1)


class TestCGDCorreosPlantillas(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_create_cgd_correos_plantilla(self):
        plantilla = CGDCorreosPlantillas(
            id_plantilla='test01',
            asunto='Asunto de prueba',
            cuerpo='Cuerpo de prueba',
            remitente='remitente@test.com',
            destinatario='destinatario@test.com',
            adjunto=False,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        self.session.add(plantilla)
        self.session.commit()
        self.assertEqual(self.session.query(CGDCorreosPlantillas).count(), 1)

    def test_create_cgd_correos_plantilla_missing_required_fields(self):
        # Intenta crear una plantilla sin el campo requerido 'asunto' (debería fallar)
        plantilla = CGDCorreosPlantillas(
            id_plantilla='test02',
            asunto=None,
            cuerpo='Cuerpo de prueba',
            remitente='remitente@test.com',
            destinatario='destinatario@test.com',
            adjunto=False,
        )
        with self.assertRaises(Exception):
            self.session.add(plantilla)
            self.session.commit()


class TestCGDCatalogoErrores(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_create_cgd_catalogo_error(self):
        error = CGDCatalogoErrores(
            codigo_error='ERR001',
            descripcion='Error de prueba',
            proceso='Proceso de prueba',
            aplica_reprogramar=True
        )
        self.session.add(error)
        self.session.commit()
        self.assertEqual(self.session.query(CGDCatalogoErrores).count(), 1)

    def test_create_cgd_catalogo_error_missing_required_fields(self):
        # Intenta crear un error sin el campo requerido 'descripcion' (debería fallar)
        error = CGDCatalogoErrores(
            codigo_error='ERR002',
            descripcion=None,  # Descripción requerida como None
            proceso='Proceso de prueba',
            aplica_reprogramar=False
        )
        with self.assertRaises(Exception):  # Espera que se lance una excepción
            self.session.add(error)
            self.session.commit()

    def test_create_cgd_catalogo_error_missing_process(self):
        # Intenta crear un error sin el campo requerido 'proceso' (debería fallar)
        error = CGDCatalogoErrores(
            codigo_error='ERR003',
            descripcion='Error de prueba',
            proceso=None,  # Proceso requerido como None
            aplica_reprogramar=True
        )
        with self.assertRaises(Exception):  # Espera que se lance una excepción
            self.session.add(error)
            self.session.commit()


class TestCGDRtaProArchivos(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        error = CGDCatalogoErrores(
            codigo_error='ERR001',
            descripcion='Error de prueba',
            proceso='Proceso de prueba',
            aplica_reprogramar=True
        )
        self.session.add(error)

        proceso = CGDRtaProcesamiento(
            id_archivo=1,
            id_rta_procesamiento=1,
            nombre_archivo_zip='archivo_test.zip',
            tipo_respuesta='01',
            fecha_recepcion=datetime.now(),
            estado='CARGADO',
            contador_intentos_cargue=0,
            codigo_error='ERR001',
            detalle_error='Sin errores'
        )
        self.session.add(proceso)
        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_create_cgd_rta_pro_archivos(self):
        rta_pro_archivos = CGDRtaProArchivos(
            id_archivo=1,
            id_rta_procesamiento=1,
            nombre_archivo='archivo_respuesta.zip',
            tipo_archivo_rta='tipo1',
            estado='CARGADO',
            contador_intentos_cargue=0,
            nro_total_registros=100,
            nro_registros_error=0,
            nro_registros_validos=100,
            codigo_error='ERR001',
            detalle_error='Sin errores'
        )
        self.session.add(rta_pro_archivos)
        self.session.commit()
        self.assertEqual(self.session.query(CGDRtaProArchivos).count(), 1)

    def test_create_cgd_rta_pro_archivos_missing_required_fields(self):
        # Intenta crear un archivo de respuesta sin el campo requerido 'nombre_archivo' (debería fallar)
        rta_pro_archivos = CGDRtaProArchivos(
            id_archivo=1,
            id_rta_procesamiento=1,
            nombre_archivo=None,
            tipo_archivo_rta='tipo1',
            estado='CARGADO',
            contador_intentos_cargue=0,
            nro_total_registros=100,
            nro_registros_error=0,
            nro_registros_validos=100,
            codigo_error='ERR001',
            detalle_error='Sin errores'
        )
        with self.assertRaises(Exception):
            self.session.add(rta_pro_archivos)
            self.session.commit()


class TestCGDRtaProcesamiento(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(cls.engine)
        cls.Session = sessionmaker(bind=cls.engine)

    def setUp(self):
        self.session = self.Session()

        # Agregar un error a la tabla cgd_catalogo_errores para poder hacer la referencia
        error = CGDCatalogoErrores(
            codigo_error='ERR001',
            descripcion='Error de prueba',
            proceso='Proceso de prueba',
            aplica_reprogramar=True
        )
        self.session.add(error)
        self.session.commit()

    def tearDown(self):
        self.session.close()

    @classmethod
    def tearDownClass(cls):
        Base.metadata.drop_all(cls.engine)

    def test_create_cgd_rta_procesamiento(self):
        # Test de creación de una instancia válida
        rta_procesamiento = CGDRtaProcesamiento(
            id_archivo=1,
            id_rta_procesamiento=1,
            nombre_archivo_zip='archivo_respuesta.zip',
            tipo_respuesta='01',
            fecha_recepcion=datetime.now(),
            estado='CARGADO',
            contador_intentos_cargue=0,
            codigo_error='ERR001',
            detalle_error='Sin errores'
        )
        self.session.add(rta_procesamiento)
        self.session.commit()

        # Verifica que se haya insertado correctamente
        result = self.session.query(CGDRtaProcesamiento).count()
        self.assertEqual(result, 1)


if __name__ == '__main__':
    unittest.main()
