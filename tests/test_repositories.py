import unittest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.aws.clients import AWSClients
from src.config.config import env
from src.models.cgd_archivo import CGDArchivoEstado, CGDArchivo
from src.models.cgd_error_catalogo import CGDCatalogoErrores
from src.models.cgd_correos_plantilla import CGDCorreosPlantillas
from src.models.cgd_correo_parametro import CGDCorreosParametros
from src.connection.database import Base
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento
from src.repositories.archivo_estado_repository import ArchivoEstadoRepository
from src.repositories.archivo_repository import ArchivoRepository
from src.repositories.catalogo_error_repository import CatalogoErrorRepository
from datetime import datetime
from src.repositories.cgd_rta_pro_archivos_repository import CGDRtaProArchivosRepository
from src.repositories.correo_parametro_repository import CorreoParametroRepository
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository
from unittest.mock import patch
from src.aws.clients import AWSClients


# class TestArchivoEstadoRepository(unittest.TestCase):
#
#     def setUp(self):
#         self.engine = create_engine('sqlite:///:memory:')
#         Base.metadata.create_all(self.engine)  # Crea todas las tablas
#         session = sessionmaker(bind=self.engine)
#         self.session = session()
#         self.repository = ArchivoEstadoRepository(self.session)
#
#         # Agregar un registro a la tabla cgd_catalogo_errores
#         error = CGDCatalogoErrores(
#             codigo_error='ERR001',
#             descripcion='Error de prueba',
#             proceso='Proceso de prueba',
#             aplica_reprogramar=True
#         )
#         self.session.add(error)
#         self.session.commit()
#
#     def tearDown(self):
#         self.session.close()  # Cierra la sesión después de cada prueba
#         Base.metadata.drop_all(self.engine)  # Limpia la base de datos en memoria
#
#     def test_insert_estado_archivo(self):
#         id_archivo = 1
#         estado_inicial = 'CREADO'
#         estado_final = 'PROCESANDO'
#
#         self.repository.insert_estado_archivo(id_archivo, estado_inicial, estado_final)
#
#         estado = self.session.query(CGDArchivoEstado).filter_by(id_archivo=id_archivo).first()
#         self.assertIsNotNone(estado)
#         self.assertEqual(estado.estado_inicial, estado_inicial)
#         self.assertEqual(estado.estado_final, estado_final)
#
#
# class TestArchivoRepository(unittest.TestCase):
#     def setUp(self):
#         self.engine = create_engine('sqlite:///:memory:')
#         Base.metadata.create_all(self.engine)  # Crea todas las tablas
#         Session = sessionmaker(bind=self.engine)
#         self.session = Session()
#         self.repository = ArchivoRepository(self.session)
#
#         # Agregar un archivo de prueba con todos los campos requeridos
#         archivo = CGDArchivo(
#             id_archivo=1,
#             nombre_archivo='archivo_prueba.txt',
#             acg_nombre_archivo='archivo_prueba.txt',
#             plataforma_origen='01',
#             tipo_archivo='tipo1',
#             consecutivo_plataforma_origen=1,
#             fecha_nombre_archivo='20240101',
#             estado='CARGADO',
#             fecha_recepcion=datetime.now(),
#             fecha_ciclo=datetime.now(),
#             contador_intentos_cargue=0,
#             contador_intentos_generacion=0,
#             contador_intentos_empaquetado=0
#         )
#         self.session.add(archivo)
#         self.session.commit()
#
#         # Agregar un registro a la tabla cgd_catalogo_errores
#         error = CGDCatalogoErrores(
#             codigo_error='ERR001',
#             descripcion='Error de prueba',
#             proceso='Proceso de prueba',
#             aplica_reprogramar=True
#         )
#         self.session.add(error)
#         self.session.commit()
#
#     def tearDown(self):
#         self.session.close()
#         Base.metadata.drop_all(self.engine)
#
#     def test_get_archivo_by_nombre_archivo(self):
#         archivo = self.repository.get_archivo_by_nombre_archivo('archivo_prueba.txt')
#         self.assertIsNotNone(archivo)
#         self.assertEqual(archivo.id_archivo, 1)
#         self.assertEqual(archivo.nombre_archivo, 'archivo_prueba.txt')
#
#     def test_check_file_exists(self):
#         exists = self.repository.check_file_exists('archivo_prueba.txt')
#         self.assertTrue(exists)
#
#     def test_update_estado_archivo(self):
#         self.repository.update_estado_archivo(
#             'archivo_prueba.txt',
#             'PROCESANDO',
#             1,
#         )
#         archivo = self.repository.get_archivo_by_nombre_archivo('archivo_prueba.txt')
#         self.assertEqual(archivo.estado, 'PROCESANDO')
#         self.assertEqual(archivo.contador_intentos_cargue, 1)
#
#     def test_check_special_file_exists(self):
#         exists = self.repository.check_special_file_exists('archivo_prueba.txt', 'tipo1')
#         self.assertTrue(exists)
#
#
# class TestCatalogoErrorRepository(unittest.TestCase):
#
#     @classmethod
#     def setUpClass(cls):
#         # Configurar la base de datos en memoria
#         cls.engine = create_engine('sqlite:///:memory:', echo=True)
#         Base.metadata.create_all(cls.engine)
#         cls.Session = sessionmaker(bind=cls.engine)
#
#     def setUp(self):
#         self.session = self.Session()
#         self.repository = CatalogoErrorRepository(self.session)
#         # Agregar datos de prueba con un valor para 'proceso'
#         error = CGDCatalogoErrores(
#             codigo_error='ERR001',
#             descripcion='Error de prueba 1',
#             proceso='Proceso de prueba',
#             aplica_reprogramar=False,
#         )
#         self.session.add(error)
#         self.session.commit()
#
#     def tearDown(self):
#         self.session.close()
#
#     @classmethod
#     def tearDownClass(cls):
#         cls.engine.dispose()
#
#     def test_get_error_by_code_existing(self):
#         """Prueba que obtenga un error existente por su código."""
#         error = self.repository.get_error_by_code('ERR001')
#         self.assertIsNotNone(error)
#         self.assertEqual(error.codigo_error, 'ERR001')
#         self.assertEqual(error.descripcion, 'Error de prueba 1')
#
#
# class TestCGDRtaProArchivosRepository(unittest.TestCase):
#
#     @classmethod
#     def setUpClass(cls):
#         # Configura una base de datos en memoria para pruebas
#         cls.engine = create_engine('sqlite:///:memory:')
#         Base.metadata.create_all(cls.engine)
#         cls.Session = sessionmaker(bind=cls.engine)
#
#     def setUp(self):
#         # Inicia una nueva sesión para cada prueba
#         self.session = self.Session()
#         self.repository = CGDRtaProArchivosRepository(self.session)
#
#         # Inserta un registro de procesamiento necesario para la clave foránea
#         self.archivo_procesamiento = CGDRtaProcesamiento(
#             id_archivo=1,
#             id_rta_procesamiento=1,
#             nombre_archivo_zip="archivo_procesado.zip",
#             tipo_respuesta="01",
#             fecha_recepcion=datetime.now(),
#             estado="PENDIENTE",
#             contador_intentos_cargue=0,
#             codigo_error=None,
#             detalle_error=None
#         )
#         self.session.add(self.archivo_procesamiento)
#         self.session.commit()  # Asegúrate de hacer commit aquí
#
#     def tearDown(self):
#         # Elimina todos los registros después de cada prueba para evitar conflictos
#         self.session.query(CGDRtaProArchivos).delete()
#         self.session.query(CGDRtaProcesamiento).delete()
#         self.session.commit()  # Asegúrate de hacer commit después de eliminar
#
#         # Cierra la sesión después de cada prueba
#         self.session.close()
#
#     def test_insert(self):
#         archivo = CGDRtaProArchivos(
#             id_archivo=1,
#             id_rta_procesamiento=1,
#             nombre_archivo="archivo_prueba.txt",
#             tipo_archivo_rta="tipo_1",
#             estado="PENDIENTE",
#             contador_intentos_cargue=0,
#             nro_total_registros=100,
#             nro_registros_error=0,
#             nro_registros_validos=100,
#             codigo_error="ERR001",
#             detalle_error=""
#         )
#
#         self.repository.insert(archivo)
#         # Verifica que el archivo se haya insertado
#         result = self.session.query(CGDRtaProArchivos).filter_by(nombre_archivo="archivo_prueba.txt").first()
#         self.assertIsNotNone(result)
#         self.assertEqual(result.estado, "PENDIENTE")
#
#     def test_get_pending_files_by_id_archivo(self):
#         # Inserta un archivo de prueba
#         archivo = CGDRtaProArchivos(
#             id_archivo=2,
#             id_rta_procesamiento=1,
#             nombre_archivo="archivo_prueba2.txt",
#             tipo_archivo_rta="tipo_1",
#             estado=env.CONST_ESTADO_INIT_PENDING,  # Asegúrate de que el estado sea correcto
#             contador_intentos_cargue=0,
#             nro_total_registros=50,
#             nro_registros_error=0,
#             nro_registros_validos=50,
#             codigo_error=None,
#             detalle_error=None
#         )
#         self.repository.insert(archivo)
#
#         # Obtiene archivos pendientes
#         pending_files = self.repository.get_pending_files_by_id_archivo(2)
#         self.assertEqual(len(pending_files), 1)
#         self.assertEqual(pending_files[0].nombre_archivo, "archivo_prueba2.txt")
#
#     def test_update_estado_to_enviado(self):
#         # Inserta un archivo de prueba
#         archivo = CGDRtaProArchivos(
#             id_archivo=3,
#             id_rta_procesamiento=1,
#             nombre_archivo="archivo_prueba3.txt",
#             tipo_archivo_rta="tipo_1",
#             estado="PENDIENTE",
#             contador_intentos_cargue=0,
#             nro_total_registros=30,
#             nro_registros_error=0,
#             nro_registros_validos=30,
#             codigo_error=None,
#             detalle_error=None
#         )
#         self.repository.insert(archivo)
#
#         # Actualiza el estado a 'ENVIADO'
#         self.repository.update_estado_to_enviado(3, "archivo_prueba3.txt")
#         updated_file = self.session.query(CGDRtaProArchivos).filter_by(nombre_archivo="archivo_prueba3.txt").first()
#         self.assertEqual(updated_file.estado, env.CONST_ESTADO_SEND)
#
#
# class TestCorreoParametroRepository(unittest.TestCase):
#
#     @classmethod
#     def setUpClass(cls):
#         # Configura una base de datos en memoria para pruebas
#         cls.engine = create_engine('sqlite:///:memory:')
#         Base.metadata.create_all(cls.engine)  # Crea todas las tablas
#         cls.Session = sessionmaker(bind=cls.engine)
#
#     def setUp(self):
#         # Inicia una nueva sesión para cada prueba
#         self.session = self.Session()
#         self.repository = CorreoParametroRepository(self.session)
#
#         # Inserta algunos datos de prueba
#         self.insert_test_data()
#
#     def tearDown(self):
#         # Elimina todos los registros después de cada prueba para evitar conflictos
#         self.session.query(CGDCorreosParametros).delete()
#         self.session.query(CGDCorreosPlantillas).delete()  # Limpia también la tabla de plantillas
#         self.session.commit()  # Asegúrate de hacer commit después de eliminar
#
#         # Cierra la sesión después de cada prueba
#         self.session.close()
#
#     def insert_test_data(self):
#         # Inserta datos de prueba en la tabla cgd_correos_plantillas
#         plantilla = CGDCorreosPlantillas(
#             id_plantilla='TPL01',
#             asunto='Asunto de prueba',
#             cuerpo='Cuerpo de prueba',
#             remitente='remitente prueba',
#             destinatario='destinatario prueba',
#             adjunto=False
#         )
#         self.session.add(plantilla)
#
#         # Inserta datos de prueba en la tabla cgd_correos_parametros
#         param1 = CGDCorreosParametros(
#             id_plantilla='TPL01',
#             id_parametro='PARAM1',
#             descripcion='Descripción del parámetro 1')
#         param2 = CGDCorreosParametros(
#             id_plantilla='TPL01',
#             id_parametro='PARAM2',
#             descripcion='Descripción del parámetro 2')
#
#         self.session.add(param1)
#         self.session.add(param2)
#         self.session.commit()  # Asegúrate de hacer commit aquí
#
#     def test_get_parameters_by_template_existing(self):
#         parameters = self.repository.get_parameters_by_template('TPL01')
#         self.assertEqual(len(parameters), 2)
#         self.assertEqual(parameters[0].id_parametro, 'PARAM1')
#         self.assertEqual(parameters[1].id_parametro, 'PARAM2')
#
#     def test_get_parameters_by_template_non_existing(self):
#         parameters = self.repository.get_parameters_by_template('TPL03')
#         self.assertEqual(len(parameters), 0)


class TestRtaProcesamientoRepository(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(cls.engine)
        cls.Session = sessionmaker(bind=cls.engine)

    @patch('src.aws.clients.AWSClients.get_ssm_client')
    def setUp(self, mock_get_ssm_client):
        mock_ssm_client = mock_get_ssm_client.return_value
        mock_ssm_client.get_parameter.return_value = {
            'Parameter': {'Value': '{"SPECIAL_START_NAME": "prefix_", "SPECIAL_END_NAME": "suffix"}'}
        }

        self.session = self.Session()
        self.repository = RtaProcesamientoRepository(self.session)
        self.insert_test_data()

    def tearDown(self):
        self.session.query(CGDRtaProcesamiento).delete()
        self.session.commit()
        self.session.close()

    def insert_test_data(self):
        rta = CGDRtaProcesamiento(
            id_archivo=1,
            id_rta_procesamiento=1,
            nombre_archivo_zip="archivo_procesado.zip",
            tipo_respuesta="01",
            fecha_recepcion=datetime.now(),
            estado=env.CONST_ESTADO_INIT_PENDING,
            contador_intentos_cargue=0,
            codigo_error=None,
            detalle_error=None
        )
        self.session.add(rta)
        self.session.commit()

    def test_get_last_contador_intentos_cargue_existing(self):
        result = self.repository.get_last_contador_intentos_cargue(id_archivo=1)
        self.assertEqual(result, 0)

    def test_update_state_rta_procesamiento(self):
        self.repository.update_state_rta_procesamiento(1, env.CONST_ESTADO_SEND)
        rta = self.session.query(CGDRtaProcesamiento).filter_by(
            id_archivo=1).order_by(CGDRtaProcesamiento.id_rta_procesamiento.desc()).first()
        self.assertEqual(rta.estado, env.CONST_ESTADO_SEND)

    def test_get_tipo_respuesta(self):
        tipo_respuesta = self.repository.get_tipo_respuesta(1)
        self.assertEqual(tipo_respuesta, "01")

    def test_get_id_rta_procesamiento(self):
        id_rta = self.repository.get_id_rta_procesamiento(1, "archivo_procesado.zip")
        self.assertEqual(id_rta, 1)

    def test_is_estado_enviado_true(self):
        self.repository.update_state_rta_procesamiento(1, env.CONST_ESTADO_SEND)
        is_enviado = self.repository.is_estado_enviado(1, "archivo_procesado.zip")
        self.assertTrue(is_enviado)

    def test_is_estado_enviado_false(self):
        is_enviado = self.repository.is_estado_enviado(1, "archivo_procesado.zip")
        self.assertFalse(is_enviado)

    def test_insert_rta_procesamiento(self):
        # Inserta el archivo de prueba
        id_archivo = 1  # Asegúrate de que este ID coincida
        archivo = CGDArchivo(
            id_archivo=id_archivo,
            nombre_archivo='archivo_prueba.txt',
            acg_nombre_archivo='archivo_prueba.txt',
            plataforma_origen='01',
            tipo_archivo='tipo1',
            consecutivo_plataforma_origen=1,
            fecha_nombre_archivo='20240101',
            estado='PENDIENTE_INICIO',
            fecha_recepcion=datetime.now(),
            fecha_ciclo=datetime.now(),
            contador_intentos_cargue=0,
            contador_intentos_generacion=0,
            contador_intentos_empaquetado=0
        )
        self.session.add(archivo)
        self.session.commit()  # Asegúrate de hacer commit para que el archivo se inserte en la DB

        # Ahora intenta insertar la respuesta de procesamiento
        self.repository.insert_rta_procesamiento(
            id_archivo=id_archivo,
            nombre_archivo_zip="nuevo_archivo.zip",
            tipo_respuesta="02",
            estado="PENDIENTE_INICIO",
            contador_intentos_cargue=0
        )

        # Verifica que la inserción se haya realizado correctamente
        result = self.session.query(CGDRtaProcesamiento).filter_by(nombre_archivo_zip="nuevo_archivo.zip").first()
        self.assertIsNotNone(result)
        self.assertEqual(result.estado, "PENDIENTE_INICIO")

        # Verifica que el archivo se actualizó correctamente
        updated_file = self.session.query(CGDArchivo).filter_by(id_archivo=id_archivo).first()
        self.assertIsNotNone(updated_file)
        self.assertEqual(updated_file.estado,
                         "PENDIENTE_INICIO")  # Asumiendo que update_estado_archivo actualiza el estado


if __name__ == '__main__':
    unittest.main()
