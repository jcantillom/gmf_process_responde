import unittest
from src.config.config import env
from src.models.cgd_archivo import CGDArchivo
from src.models.cgd_error_catalogo import CGDCatalogoErrores
from src.models.cgd_correo_parametro import CGDCorreosParametros
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.repositories.archivo_estado_repository import ArchivoEstadoRepository
from src.repositories.archivo_repository import ArchivoRepository
from src.repositories.catalogo_error_repository import CatalogoErrorRepository
from datetime import datetime
from src.repositories.cgd_rta_pro_archivos_repository import CGDRtaProArchivosRepository
from src.repositories.correo_parametro_repository import CorreoParametroRepository
from unittest.mock import patch, MagicMock
from sqlalchemy.orm import Session
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository
from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento
from sqlalchemy.exc import IntegrityError


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

    def test_insert_estado_archivo_integrity_error_duplicate_key(self):
        # Simular un IntegrityError con detalle de clave duplicada
        error = IntegrityError(
            statement="INSERT INTO cgd_archivo_estado ...",
            params=None,
            orig=Exception("duplicate key value violates unique constraint 'unique_estado'")
        )
        self.mock_db.commit.side_effect = error

        # Ejecutar la función y verificar que se lanza la excepción adecuada
        with self.assertRaises(ValueError) as context:
            self.repo.insert_estado_archivo(1, 'PENDIENTE', 'PROCESANDO')

        # Verificar el mensaje de la excepción
        self.assertIn("Error de clave duplicada", str(context.exception))
        self.mock_db.rollback.assert_called_once()

    def test_insert_estado_archivo_integrity_error_other(self):
        # Simular un IntegrityError sin detalle de clave duplicada
        error = IntegrityError(
            statement="INSERT INTO cgd_archivo_estado ...",
            params=None,
            orig=Exception("some other database error")
        )
        self.mock_db.commit.side_effect = error

        # Ejecutar la función y verificar que se lanza la excepción adecuada
        with self.assertRaises(ValueError) as context:
            self.repo.insert_estado_archivo(1, 'PENDIENTE', 'PROCESANDO')

        # Verificar el mensaje de la excepción
        self.assertIn("Error en la base de datos", str(context.exception))
        self.mock_db.rollback.assert_called_once()


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

    def test_insert_code_error(self):
        # Configurar el archivo existente en la base de datos
        id_archivo = 1
        archivo_existente = CGDArchivo(id_archivo=id_archivo)

        # Configurar el código de error y el detalle del error
        code_error = "ER001"
        detail_error = "Error de prueba"

        # Configurar el comportamiento del mock
        self.mock_db.query.return_value.filter.return_value.first.return_value = archivo_existente

        # Llamar al método que estamos probando
        self.repo.insert_code_error(id_archivo, code_error, detail_error)

        # Verificar que el código de error y el detalle del error se actualizaron correctamente
        self.assertEqual(archivo_existente.codigo_error, code_error)
        self.assertEqual(archivo_existente.detalle_error, detail_error)

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


class TestCGDRtaProArchivosRepository(unittest.TestCase):
    def setUp(self):
        # Crear un mock de la sesión de la base de datos
        self.db_mock = MagicMock(spec=Session)
        self.repository = CGDRtaProArchivosRepository(self.db_mock)

    def test_get_pending_files_by_id_archivo(self):
        # Datos de prueba
        id_archivo = 123

        # Configurar el mock para devolver un archivo pendiente
        mock_file = MagicMock(spec=CGDRtaProArchivos)
        self.db_mock.query().filter().all.return_value = [mock_file]

        # Llamar a la función
        result = self.repository.get_pending_files_by_id_archivo(id_archivo)

        # Verificar el resultado
        self.assertEqual(result, [mock_file])
        self.db_mock.query().filter().all.assert_called_once()

    def test_update_estado_to_enviado_existing_file(self):
        # Datos de prueba
        id_archivo = 123
        nombre_archivo = "test_file.txt"

        # Configurar el mock para devolver un archivo existente
        mock_file = MagicMock(spec=CGDRtaProArchivos)
        self.db_mock.query().filter().first.return_value = mock_file

        # Llamar a la función
        self.repository.update_estado_to_enviado(id_archivo, nombre_archivo, id_rta_procesamiento=456)

        # Verificar que el estado se actualizó y que se llamó a commit
        self.assertEqual(mock_file.estado, env.CONST_ESTADO_SEND)
        self.db_mock.commit.assert_called_once()

    def test_update_estado_to_enviado_no_file(self):
        # Datos de prueba
        id_archivo = 123
        nombre_archivo = "test_file.txt"

        # Configurar el mock para devolver None (archivo no encontrado)
        self.db_mock.query().filter().first.return_value = None

        # Llamar a la función
        self.repository.update_estado_to_enviado(id_archivo, nombre_archivo, id_rta_procesamiento=456)

        # Verificar que no se llamó a commit
        self.db_mock.commit.assert_not_called()

    def test_get_files_loaded_for_response(self):
        # Datos de prueba
        id_archivo = 123
        id_rta_procesamiento = 456

        # Configurar el mock para devolver archivos cargados
        mock_file = MagicMock(spec=CGDRtaProArchivos)
        self.db_mock.query().filter().all.return_value = [mock_file]

        # Llamar a la función
        result = self.repository.get_files_loaded_for_response(id_archivo, id_rta_procesamiento)

        # Verificar el resultado
        self.assertEqual(result, [mock_file])
        self.db_mock.query().filter().all.assert_called_once()


class TestRtaProcesamientoRepository(unittest.TestCase):
    @patch(
        'src.core.validator.ArchivoValidator._get_file_config_name',
        return_value=("dummy_config", "dummy_dir", "dummy_prefix", "dummy_suffix"))
    def setUp(self, mock_get_file_config_name):
        # Crear un mock de la sesión de la base de datos
        self.db_mock = MagicMock(spec=Session)
        self.repository = RtaProcesamientoRepository(self.db_mock)

    def test_update_contador_intentos_cargue(self):
        # Datos de prueba
        id_archivo = 123
        contador_intentos_cargue = 5

        # Crear un mock del objeto CGDRtaProcesamiento
        mock_rta_procesamiento = MagicMock(spec=CGDRtaProcesamiento)
        mock_rta_procesamiento.id_archivo = id_archivo
        mock_rta_procesamiento.contador_intentos_cargue = 3  # Valor inicial

        # Configurar el mock para get_last_rta_procesamiento
        self.repository.get_last_rta_procesamiento = MagicMock(return_value=mock_rta_procesamiento)

        # Llamar a la función
        self.repository.update_contador_intentos_cargue(id_archivo, contador_intentos_cargue)

        # Verificar que se actualizó el valor
        self.assertEqual(mock_rta_procesamiento.contador_intentos_cargue, contador_intentos_cargue)

        # Verificar que se llamaron commit y refresh
        self.db_mock.commit.assert_called_once()
        self.db_mock.refresh.assert_called_once_with(mock_rta_procesamiento)

        # Verificar que get_last_rta_procesamiento fue llamado con el id correcto
        self.repository.get_last_rta_procesamiento.assert_called_once_with(id_archivo)
