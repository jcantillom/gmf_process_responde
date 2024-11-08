# import unittest
# from datetime import datetime
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from src.models.base import Base
# from src.models.cgd_archivo import CGDArchivo, CGDArchivoEstado
# from src.models.cgd_correo_parametro import CGDCorreosParametros
# from src.models.cgd_correos_plantilla import CGDCorreosPlantillas
# from src.models.cgd_error_catalogo import CGDCatalogoErrores
#
#
# class TestCGDArchivo(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls):
#         # Configurar el motor y la sesión
#         cls.engine = create_engine('sqlite:///:memory:')
#         Base.metadata.bind = cls.engine
#         cls.Session = sessionmaker(bind=cls.engine)
#
#         # Crear todas las tablas necesarias antes de inicializar la sesión
#         Base.metadata.create_all(cls.engine)
#
#         # Crear una sesión para operaciones en setUpClass
#         cls.session = cls.Session()
#
#         # Insertar datos necesarios para las pruebas
#         cls.insertar_datos_iniciales(cls.session)
#
#     @classmethod
#     def insertar_datos_iniciales(cls, session):
#         # Agregar un error al catálogo para todas las pruebas
#         error = CGDCatalogoErrores(
#             codigo_error='ERR001',
#             descripcion='Error de prueba',
#             proceso='Proceso de prueba',
#             aplica_reprogramar=True
#         )
#         session.add(error)
#         session.commit()
#
#         # Insertar plantilla de correos para pruebas de parámetros
#         plantilla = CGDCorreosPlantillas(nombre_plantilla='Plantilla de prueba')
#         session.add(plantilla)
#         session.commit()
#
#     def setUp(self):
#         self.session = self.Session()
#
#     def tearDown(self):
#         self.session.close()
#
#     @classmethod
#     def tearDownClass(cls):
#         cls.session.close()
#         Base.metadata.drop_all(cls.engine)
#
#     def test_create_cgd_archivo(self):
#         archivo = CGDArchivo(
#             id_archivo=1,
#             nombre_archivo='archivo.txt',
#             plataforma_origen='01',
#             tipo_archivo='01',
#             consecutivo_plataforma_origen='0001',
#             fecha_nombre_archivo='20210101',
#             fecha_registro_resumen='20210101120000',
#             nro_total_registros=100,
#             nro_registros_error=0,
#             nro_registros_validos=100,
#             estado='CARGADO',
#             fecha_recepcion=datetime.now(),
#             fecha_ciclo=datetime.now().date(),
#             contador_intentos_cargue=0,
#             contador_intentos_generacion=0,
#             contador_intentos_empaquetado=0,
#             acg_fecha_generacion=datetime.now(),
#             acg_consecutivo=1,
#             acg_nombre_archivo='archivo.txt',
#             acg_registro_encabezado='encabezado',
#             acg_registro_resumen='resumen',
#             acg_total_tx=100,
#             acg_monto_total_tx=100.00,
#             acg_total_tx_debito=0,
#             acg_monto_total_tx_debito=0.00,
#             acg_total_tx_reverso=0,
#             acg_monto_total_tx_reverso=0.00,
#             acg_total_tx_reintegro=0,
#             acg_monto_total_tx_reintegro=0.00,
#             anulacion_nombre_archivo="anulacion.txt",
#             anulacion_justificacion="justificacion",
#             anulacion_fecha_anulacion=datetime.now(),
#             gaw_rta_trans_estado="estado",
#             gaw_rta_trans_codigo="codigo",
#             gaw_rta_trans_detalle="detalle",
#             codigo_error="ERR001",
#             detalle_error="Sin errores"
#         )
#         self.session.add(archivo)
#         self.session.commit()
#         self.assertEqual(self.session.query(CGDArchivo).count(), 1)
#
#     def test_create_cgd_archivo_estado(self):
#         archivo = CGDArchivo(
#             id_archivo=2,
#             nombre_archivo='archivo_estado.txt',
#             plataforma_origen='01',
#             tipo_archivo='01',
#             consecutivo_plataforma_origen='0002',
#             fecha_nombre_archivo='20210201',
#             fecha_registro_resumen='20210201120000',
#             nro_total_registros=100,
#             nro_registros_error=0,
#             nro_registros_validos=100,
#             estado='CARGADO',
#             fecha_recepcion=datetime.now(),
#             fecha_ciclo=datetime.now().date(),
#             contador_intentos_cargue=0,
#             contador_intentos_generacion=0,
#             contador_intentos_empaquetado=0,
#             acg_fecha_generacion=datetime.now(),
#             acg_consecutivo=2,
#             acg_nombre_archivo='archivo_estado.txt',
#             acg_registro_encabezado='encabezado',
#             acg_registro_resumen='resumen',
#             acg_total_tx=100,
#             acg_monto_total_tx=100.00,
#             acg_total_tx_debito=0,
#             acg_monto_total_tx_debito=0.00,
#             acg_total_tx_reverso=0,
#             acg_monto_total_tx_reverso=0.00,
#             acg_total_tx_reintegro=0,
#             acg_monto_total_tx_reintegro=0.00,
#             anulacion_nombre_archivo="anulacion_estado.txt",
#             anulacion_justificacion="justificacion",
#             anulacion_fecha_anulacion=datetime.now(),
#             gaw_rta_trans_estado="estado",
#             gaw_rta_trans_codigo="codigo",
#             gaw_rta_trans_detalle="detalle",
#             codigo_error="ERR002",
#             detalle_error="Sin errores"
#         )
#         self.session.add(archivo)
#         self.session.commit()
#
#         estado_archivo = CGDArchivoEstado(
#             id_archivo=archivo.id_archivo,
#             estado_inicial='CARGADO',
#             estado_final='PROCESANDO',
#             fecha_cambio_estado=datetime.now()
#         )
#         self.session.add(estado_archivo)
#         self.session.commit()
#         self.assertEqual(self.session.query(CGDArchivoEstado).count(), 1)
