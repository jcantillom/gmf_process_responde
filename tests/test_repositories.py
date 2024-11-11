import unittest
from src.config.config import env
from src.models.cgd_archivo import CGDArchivoEstado, CGDArchivo
from src.models.cgd_error_catalogo import CGDCatalogoErrores
from src.models.cgd_correo_parametro import CGDCorreosParametros
from src.models.base import Base
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento
from src.repositories.archivo_estado_repository import ArchivoEstadoRepository
from src.repositories.archivo_repository import ArchivoRepository
from src.repositories.catalogo_error_repository import CatalogoErrorRepository
from datetime import datetime
from src.repositories.cgd_rta_pro_archivos_repository import CGDRtaProArchivosRepository
from src.repositories.correo_parametro_repository import CorreoParametroRepository
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository
from unittest.mock import patch, MagicMock
from src.aws.clients import AWSClients
from sqlalchemy.orm import Session
from unittest.mock import ANY


class TestArchivoEstadoRepository(unittest.TestCase):
    def setUp(self):
        # Configurar un objeto de sesión de base de datos simulado
        self.mock_db = MagicMock()
        self.repo = ArchivoEstadoRepository(db=self.mock_db)

    @patch('src.models.cgd_archivo.CGDArchivoEstado')
    def test_insert_estado_archivo(self, mock_archivo_estado):
        # Configurar el objeto de archivo de estado simulado
        mock_archivo_estado.return_value = MagicMock()
        mock_archivo_estado.return_value.id_archivo = 1
        mock_archivo_estado.return_value.estado_inicial = 'PENDIENTE'
        mock_archivo_estado.return_value.estado_final = 'PROCESANDO'
        mock_archivo_estado.return_value.fecha_cambio_estado = datetime.now()

        # Insertar un nuevo estado de archivo
        self.repo.insert_estado_archivo(1, 'PENDIENTE', 'PROCESANDO')

        # Verificar que se haya llamado a add, commit y refresh
        self.mock_db.add.assert_called_once()
        self.mock_db.commit.assert_called_once()
        self.mock_db.refresh.assert_called_once()


class TestArchivoRepository(unittest.TestCase):
    def setUp(self):
        # Configurar un objeto de sesión de base de datos simulado
        self.mock_db = MagicMock(spec=Session)
        self.repo = ArchivoRepository(db=self.mock_db)

    def test_get_archivo_by_nombre_archivo_found(self):
        # Datos de prueba
        nombre_archivo = "test_file.txt"
        expected_archivo = CGDArchivo(id_archivo=1, acg_nombre_archivo=nombre_archivo)

        self.mock_db.query.return_value.filter.return_value.first.return_value = expected_archivo

        result = self.repo.get_archivo_by_nombre_archivo(nombre_archivo)

        # Verificar que el resultado sea el archivo esperado
        self.assertEqual(result, expected_archivo)

    def test_get_archivo_by_nombre_archivo_not_found(self):
        nombre_archivo = "non_existent_file.txt"

        self.mock_db.query.return_value.filter.return_value.first.return_value = None

        result = self.repo.get_archivo_by_nombre_archivo(nombre_archivo)

        # Verificar que el resultado sea None
        self.assertIsNone(result)

    def test_check_file_exists_true(self):
        nombre_archivo = "test_file.txt"
        expected_archivo = CGDArchivo(id_archivo=1, acg_nombre_archivo=nombre_archivo)

        self.mock_db.query.return_value.filter.return_value.first.return_value = expected_archivo

        result = self.repo.check_file_exists(nombre_archivo)

        # Verificar que el resultado sea True
        self.assertTrue(result)

    def test_check_file_exists_false(self):
        nombre_archivo = "non_existent_file.txt"

        self.mock_db.query.return_value.filter.return_value.first.return_value = None

        result = self.repo.check_file_exists(nombre_archivo)

        # Verificar que el resultado sea False
        self.assertFalse(result)

    def test_update_estado_archivo(self):
        nombre_archivo = "test_file.txt"
        estado_nuevo = "CARGADO"
        contador_intentos_cargue = 2
        archivo_existente = CGDArchivo(id_archivo=1, acg_nombre_archivo=nombre_archivo, estado="INICIADO",
                                       contador_intentos_cargue=1)

        self.mock_db.query.return_value.filter.return_value.first.return_value = archivo_existente

        self.repo.update_estado_archivo(nombre_archivo, estado_nuevo, contador_intentos_cargue)

        # Verificar que el estado y el contador se actualizaron correctamente
        self.assertEqual(archivo_existente.estado, estado_nuevo)
        self.assertEqual(archivo_existente.contador_intentos_cargue, contador_intentos_cargue)

        # Verificar que se haya hecho un commit en la base de datos
        self.mock_db.commit.assert_called_once()

    def test_check_special_file_exists_true(self):
        acg_nombre_archivo = "special_file.txt"
        tipo_archivo = "01"
        expected_archivo = CGDArchivo(id_archivo=1, acg_nombre_archivo=acg_nombre_archivo, tipo_archivo=tipo_archivo)

        self.mock_db.query.return_value.filter.return_value.first.return_value = expected_archivo

        result = self.repo.check_special_file_exists(acg_nombre_archivo, tipo_archivo)

        # Verificar que el resultado sea True
        self.assertTrue(result)

    def test_check_special_file_exists_false(self):
        acg_nombre_archivo = "non_existent_special_file.txt"
        tipo_archivo = "01"

        # Simular el comportamiento del método de consulta
        self.mock_db.query.return_value.filter.return_value.first.return_value = None

        # Llamar al método que estamos probando
        result = self.repo.check_special_file_exists(acg_nombre_archivo, tipo_archivo)

        # Verificar que el resultado sea False
        self.assertFalse(result)

    def test_insert_archivo(self):
        archivo = CGDArchivo(id_archivo=1, acg_nombre_archivo="new_file.txt")

        # Llamar al método que estamos probando
        self.repo.insert_archivo(archivo)

        # Verificar que se agregó el archivo a la base de datos
        self.mock_db.add.assert_called_once_with(archivo)

        # Verificar que se hizo commit a la base de datos
        self.mock_db.commit.assert_called_once()


class TestCatalogoErrorRepository(unittest.TestCase):

    def setUp(self):
        # Crear un mock de la sesión de la base de datos
        self.mock_db = MagicMock(spec=Session)
        self.repo = CatalogoErrorRepository(self.mock_db)

    def test_get_error_by_code_not_found(self):
        # Datos de prueba
        codigo_error = "ER002"
        expected_archivo = CGDCatalogoErrores(codigo_error=codigo_error)

        # Configurar el comportamiento del mock
        self.mock_db.query.return_value.filter.return_value.first.return_value = expected_archivo

        result = self.repo.get_error_by_code(codigo_error)

        # Verificar que el resultado sea el error esperado
        self.assertEqual(result, expected_archivo)


class TestCGDRtaProArchivosRepository(unittest.TestCase):

    def setUp(self):
        # Configura una base de datos en memoria para pruebas
        self.mock_db = MagicMock(spec=Session)
        self.repo = CGDRtaProArchivosRepository(self.mock_db)

    def test_insert(self):
        # Configura un archivo de prueba
        archivo = CGDRtaProArchivos(
            id_archivo=1,
            id_rta_procesamiento=1,
            nombre_archivo="archivo_prueba.txt",
            tipo_archivo_rta="tipo_1",
            estado=env.CONST_ESTADO_INIT_PENDING,  # Asegúrate de que el estado sea correcto
            contador_intentos_cargue=0,
            nro_total_registros=100,
            nro_registros_error=0,
            nro_registros_validos=100,
            codigo_error=None,
            detalle_error=None
        )

        # Inserta el archivo de prueba
        self.repo.insert(archivo)

        # Verifica que se haya llamado a add, commit y refresh
        self.mock_db.add.assert_called_once()
        self.mock_db.commit.assert_called_once()
        self.mock_db.refresh.assert_called_once()

    def test_get_pending_files_by_id_archivo(self):
        id_archivo = 1
        expected_file = CGDRtaProArchivos(id_archivo=id_archivo)

        self.mock_db.query.return_value.filter.return_value.all.return_value = expected_file

        result = self.repo.get_pending_files_by_id_archivo(id_archivo)

        self.assertEqual(result, expected_file)

    def test_update_estado_to_enviado(self):
        id_archivo = 1
        nombre_archivo = "archivo_prueba.txt"
        expected_file = CGDRtaProArchivos(id_archivo=id_archivo, nombre_archivo=nombre_archivo)

        self.mock_db.query.return_value.filter.return_value.first.return_value = expected_file

        self.repo.update_estado_to_enviado(id_archivo, nombre_archivo)

        self.assertEqual(expected_file.estado, env.CONST_ESTADO_SEND)
        self.mock_db.commit.assert_called_once()


class TestCorreoParametroRepository(unittest.TestCase):

    def setUp(self):
        # Inicia una nueva sesión para cada prueba
        self.mock_db = MagicMock(spec=Session)
        self.repository = CorreoParametroRepository(self.mock_db)

    def test_get_parameters_by_template(self):
        # Configura los parámetros de prueba
        id_plantilla = "plantilla_1"
        expected_parameters = [
            CGDCorreosParametros(id_plantilla=id_plantilla, descripcion="parametro_1"),
            CGDCorreosParametros(id_plantilla=id_plantilla, descripcion="parametro_2")
        ]

        # Configura el comportamiento del mock
        self.mock_db.query.return_value.filter.return_value.all.return_value = expected_parameters

        result = self.repository.get_parameters_by_template(id_plantilla)

        # Verifica que el resultado sea el esperado
        self.assertEqual(result, expected_parameters)

#
