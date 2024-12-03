import json
from io import BytesIO
from zipfile import ZipFile, BadZipFile

from src.config.config import env
from src.core.process_event import (
    extract_filename_from_body,
    extract_bucket_from_body,
    extract_date_from_filename,
    create_file_id,
    extract_consecutivo_plataforma_origen,
    build_acg_name_if_general_file,
    extract_and_validate_event_data,
)
from src.services import s3_service
from src.utils.sqs_utils import (
    delete_message_from_sqs,
    send_message_to_sqs,
    build_email_message,
    send_message_to_sqs_with_delay,
)
from src.utils.singleton import SingletonMeta
from src.services.error_handling_service import ErrorHandlingService
import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
from src.services.s3_service import S3Utils
from src.core.custom_error import CustomFunctionError
from unittest.mock import patch
from datetime import datetime
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository


class Singleton(metaclass=SingletonMeta):
    pass


class TestEventUtils(unittest.TestCase):

    def test_extract_filename_from_body(self):
        # Test con un cuerpo de mensaje válido
        body = json.dumps({
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": "Recibidos/test_file.zip"
                        },
                        "bucket": {
                            "name": "01-bucketrtaprocesa-d01"
                        }
                    }
                }
            ]
        })

        expected_filename = "test_file.zip"
        actual_filename = extract_filename_from_body(body)
        self.assertEqual(actual_filename, expected_filename)

    def test_extract_bucket_from_body(self):
        # Test con un cuerpo de mensaje válido
        body = json.dumps({
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": "Recibidos/test_file.zip"
                        },
                        "bucket": {
                            "name": "01-bucketrtaprocesa-d01"
                        }
                    }
                }
            ]
        })

        expected_bucket_name = "01-bucketrtaprocesa-d01"
        actual_bucket_name = extract_bucket_from_body(body)
        self.assertEqual(actual_bucket_name, expected_bucket_name)

    def test_extract_filename_from_body_invalid_json(self):
        # Test con un cuerpo de mensaje no válido (no es JSON)
        body = "invalid_json"
        with self.assertRaises(json.JSONDecodeError):
            extract_filename_from_body(body)

    def test_extract_bucket_from_body_invalid_json(self):
        # Test con un cuerpo de mensaje no válido (no es JSON)
        body = "invalid_json"
        with self.assertRaises(json.JSONDecodeError):
            extract_bucket_from_body(body)

    def test_extract_filename_from_body_missing_key(self):
        # Test con un cuerpo de mensaje sin la clave 'key'
        body = json.dumps({
            "Records": [
                {
                    "s3": {
                        "object": {},
                        "bucket": {
                            "name": "01-bucketrtaprocesa-d01"
                        }
                    }
                }
            ]
        })
        with self.assertRaises(KeyError):
            extract_filename_from_body(body)

    def test_extract_bucket_from_body_missing_bucket_name(self):
        # Test con un cuerpo de mensaje sin el nombre del bucket
        body = json.dumps({
            "Records": [
                {
                    "s3": {
                        "object": {
                            "key": "Recibidos/test_file.zip"
                        },
                        "bucket": {}
                    }
                }
            ]
        })
        with self.assertRaises(KeyError):
            extract_bucket_from_body(body)

    @patch('src.core.process_event.env')
    def test_extract_date_from_filename_valid(self, mock_env):
        # Configurar el prefijo del archivo especial en la variable de entorno
        mock_env.CONST_PRE_SPECIAL_FILE = "RE_ESP"
        filename = "RE_ESP_TUTGMF0001003920241002-0001.zip"

        # Ejecutar y verificar que la fecha extraída es correcta
        result = extract_date_from_filename(filename)
        self.assertEqual(result, "20241002")

    @patch('src.core.process_event.env')
    def test_extract_date_from_filename_invalid(self, mock_env):
        # Configurar el prefijo del archivo especial en la variable de entorno
        mock_env.CONST_PRE_SPECIAL_FILE = "RE_ESP"
        filename = "RE_ESP_WRONG_FORMAT.zip"

        # Ejecutar y verificar que devuelve una cadena vacía para formato incorrecto
        result = extract_date_from_filename(filename)
        self.assertEqual(result, "")

    @patch('src.core.validator.AWSClients.get_ssm_client')  # Mock del cliente SSM
    @patch('src.core.validator.ArchivoValidator')  # Mock de la clase ArchivoValidator
    @patch('src.core.process_event.env')  # Mock de las variables de entorno
    @patch('src.core.process_event.extract_date_from_filename')  # Mock de la función extract_date_from_filename
    def test_create_file_id_with_valid_date(self, mock_extract_date, mock_env, mock_validator, mock_get_ssm_client):
        """
        Test que verifica la creación de un ID de archivo con una fecha válida.
        """
        # Mock de la respuesta de SSM (simula la respuesta de get_parameter)
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {
            'Parameter': {
                'Value': json.dumps({
                    "files-reponses-debito-reverso": "01,02",
                    "files-reponses-reintegros": "03,04",
                    "files-reponses-especiales": "05,06",
                    "SPECIAL_START_NAME": "RE_ESP",
                    "SPECIAL_END_NAME": "0001"
                })
            }
        }
        mock_get_ssm_client.return_value = mock_ssm

        # Configurar el entorno
        mock_env.CONST_PLATAFORMA_ORIGEN = '01'
        mock_env.CONST_TIPO_ARCHIVO_ESPECIAL = '03'
        mock_env.CONST_PRE_SPECIAL_FILE = 'RE_ESP'

        # Configurar el mock de ArchivoValidator para devolver special_end como '0001'
        mock_validator_instance = mock_validator.return_value
        mock_validator_instance.special_end = '0001'

        # Mock de la función extract_date_from_filename
        filename = "RE_ESP_TUTGMF0001003920241002-0001.zip"
        mock_extract_date.return_value = '20241002'  # Simulando una fecha válida extraída del nombre del archivo

        # Ejecutar la función que estamos probando
        result = create_file_id(filename)

        # Verificar que el ID de archivo generado es correcto
        self.assertEqual(result, 2024100201030000)

    @patch('src.core.process_event.env')
    def test_extract_consecutivo_plataforma_origen(self, mock_env):
        # Configurar el prefijo del archivo especial en la variable de entorno
        mock_env.CONST_PRE_SPECIAL_FILE = "RE_ESP"
        filename = "RE_ESP_TUTGMF0001003920241002-0001.zip"

        # Ejecutar y verificar que el consecutivo extraído es correcto
        result = extract_consecutivo_plataforma_origen(filename)
        self.assertEqual(result, "0001")

    @patch('src.core.process_event.env')
    def test_build_acg_name_if_general_file(self, mock_env):
        # Configurar el prefijo del archivo general en la variable de entorno
        mock_env.CONST_PRE_GENERAL_FILE = "RE_GEN"
        acg_nombre_archivo = "RE_GEN_TUTGMF0001003920241002-0001.zip"

        # Ejecutar y verificar que el nombre del archivo ACG generado es correcto
        result = build_acg_name_if_general_file(acg_nombre_archivo)
        self.assertEqual(result, "TUTGMF0001003920241002-0001.zip")


class TestSingleton(unittest.TestCase):

    def test_singleton_instance(self):
        # Crea dos instancias de la clase Singleton
        instance1 = Singleton()
        instance2 = Singleton()

        # Verifica que ambas instancias son la misma
        self.assertIs(instance1, instance2, "Se esperaban las mismas instancias, pero se obtuvieron diferentes.")

    def test_singleton_properties(self):
        # Añadir propiedades a la instancia
        instance = Singleton()
        instance.value = "Singleton Value"

        # Verificar que la propiedad persiste en la instancia
        self.assertEqual(instance.value, "Singleton Value")

        # Verificar que la propiedad también está en la otra instancia
        another_instance = Singleton()
        self.assertEqual(another_instance.value, "Singleton Value")

    def test_different_singleton_classes(self):
        class AnotherSingleton(metaclass=SingletonMeta):
            pass

        # Crear instancias de diferentes clases Singleton
        singleton_instance = Singleton()
        another_singleton_instance = AnotherSingleton()

        # Asegurarse de que son diferentes instancias
        self.assertIsNot(singleton_instance, another_singleton_instance,
                         "Se esperaban instancias diferentes para diferentes clases Singleton.")


class TestSQSUtils(unittest.TestCase):

    @patch('src.services.aws_clients_service.AWSClients.get_sqs_client')
    def test_send_message_to_sqs(self, mock_get_sqs_client):
        # Configura el mock
        mock_sqs = MagicMock()
        mock_get_sqs_client.return_value = mock_sqs

        queue_url = 'http://example.com/sqs'
        message_body = {'key': 'value'}
        filename = 'test_file.txt'

        # Llama a la función
        send_message_to_sqs(queue_url, message_body, filename)

        # Afirmaciones
        mock_sqs.send_message.assert_called_once_with(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body, ensure_ascii=False)
        )

    @patch('src.services.aws_clients_service.AWSClients.get_sqs_client')
    def test_delete_message_from_sqs(self, mock_get_sqs_client):
        # Configura el mock
        mock_sqs = MagicMock()
        mock_get_sqs_client.return_value = mock_sqs

        receipt_handle = 'test_receipt_handle'
        queue_url = 'http://example.com/sqs'
        filename = 'test_file.txt'

        # Llama a la función
        delete_message_from_sqs(receipt_handle, queue_url, filename)

        # Afirmaciones
        mock_sqs.delete_message.assert_called_once_with(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

    @patch('src.services.aws_clients_service.AWSClients.get_sqs_client')
    def test_delete_message_from_sqs_error(self, mock_get_sqs_client):
        # Configura el mock
        mock_sqs = MagicMock()
        mock_sqs.delete_message.side_effect = ClientError({}, 'delete_message')
        mock_get_sqs_client.return_value = mock_sqs

        receipt_handle = 'test_receipt_handle'
        queue_url = 'http://example.com/sqs'
        filename = 'test_file.txt'

        # Llama a la función
        delete_message_from_sqs(receipt_handle, queue_url, filename)

        # Afirmaciones
        mock_sqs.delete_message.assert_called_once_with(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

    @patch('src.services.aws_clients_service.AWSClients.get_sqs_client')
    def test_send_message_to_sqs_error(self, mock_get_sqs_client):
        # Configura el mock
        mock_sqs = MagicMock()
        mock_sqs.send_message.side_effect = ClientError({}, 'send_message')
        mock_get_sqs_client.return_value = mock_sqs

        queue_url = 'http://example.com/sqs'
        message_body = {'key': 'value'}
        filename = 'test_file.txt'

        # Llama a la función
        send_message_to_sqs(queue_url, message_body, filename)

        # Afirmaciones
        mock_sqs.send_message.assert_called_once_with(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body, ensure_ascii=False)
        )

    def test_build_email_message(self):
        message = {
            "id_plantilla": "PC009",
            "parametros": []
        }

        id_plantilla = "PC009"
        error_data = {}
        mail_parameters = []
        filename = "test_file.txt"

        actual_message = build_email_message(id_plantilla, error_data, mail_parameters, filename)
        self.assertEqual(actual_message, message)

    @patch('src.services.aws_clients_service.AWSClients.get_sqs_client')
    def test_send_message_to_sqs_with_delay(self, mock_get_sqs_client):
        # Configura el mock
        mock_sqs = MagicMock()
        mock_get_sqs_client.return_value = mock_sqs

        queue_url = 'http://example.com/sqs'
        message_body = {'key': 'value'}
        filename = 'test_file.txt'
        delay_seconds = 10

        # Llama a la función
        send_message_to_sqs_with_delay(queue_url, message_body, filename, delay_seconds)

        # Afirmaciones
        mock_sqs.send_message.assert_called_once_with(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body, ensure_ascii=False),
            DelaySeconds=delay_seconds
        )


class TestS3Utils(unittest.TestCase):
    @patch('boto3.client')
    @patch('src.services.aws_clients_service.AWSClients.get_ssm_client')
    @patch('src.services.aws_clients_service.AWSClients.get_s3_client')
    def setUp(self, mock_get_s3_client, mock_get_ssm_client, mock_boto_client):
        # Crear mocks para los clientes
        mock_ssm_client = MagicMock()
        mock_s3_client = MagicMock()

        # Configurar los retornos de los mocks
        mock_get_ssm_client.return_value = mock_ssm_client
        mock_get_s3_client.return_value = mock_s3_client

        mock_ssm_client.get_parameter.return_value = {
            'Parameter': {
                'Value': '{"special_start": "RE_ESP",'
                         ' "special_end": "0001", '
                         '"general_start": "RE_GEN", '
                         '"files-reponses-debito-reverso": "001,002", '
                         '"files-reponses-reintegros": "R", '
                         '"files-reponses-especiales": "ESP"}'
            }
        }

        # Crear instancias mock para S3 y el servicio de manejo de errores
        self.s3_utils = S3Utils(MagicMock())
        self.error_handling_service = MagicMock(spec=ErrorHandlingService)

        # Mockear métodos de S3
        self.s3_utils.s3 = mock_s3_client
        self.s3_utils.s3.get_object = MagicMock()
        self.s3_utils.s3.upload_fileobj = MagicMock()
        self.s3_utils.s3.delete_object = MagicMock()
        self.s3_utils.s3.copy_object = MagicMock()

        # Crear un archivo zip válido en memoria para las pruebas
        self.mock_zip_content = BytesIO()
        with ZipFile(self.mock_zip_content, 'w') as zip_file:
            zip_file.writestr('file1-01.txt', 'Contenido del archivo 1')
            zip_file.writestr('file2-01.txt', 'Contenido del archivo 2')
        self.mock_zip_content.seek(0)

        # Mockear el retorno de get_object
        self.s3_utils.s3.get_object.return_value = {'Body': self.mock_zip_content}

        # Mockear la cantidad esperada de archivos
        self.s3_utils.get_cantidad_de_archivos_esperados_en_el_zip = MagicMock(return_value=(2, '01'))

    def test_check_file_exists_in_s3_exists(self):
        # Simular que el archivo existe en S3
        self.s3_utils.s3.head_object.return_value = {}
        result = self.s3_utils.check_file_exists_in_s3('test-bucket', 'test-key')
        self.assertTrue(result)

    def test_check_file_exists_in_s3_not_exists(self):
        # Simular que el archivo no existe en S3
        self.s3_utils.s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
        result = self.s3_utils.check_file_exists_in_s3('test-bucket', 'test-key')
        self.assertFalse(result)

    @patch("src.services.s3_service.datetime")
    def test_move_file_to_rechazados_success(self, mock_datetime):
        # Simular una fecha específica
        mock_datetime.now.return_value = datetime(2024, 11, 1)
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        # Simular que el archivo existe
        self.s3_utils.check_file_exists_in_s3 = MagicMock(return_value=True)

        # Resetear los mocks para asegurarse de que no hay llamadas previas
        self.s3_utils.s3.copy_object.reset_mock()
        self.s3_utils.s3.delete_object.reset_mock()

        # Simular el movimiento de archivo
        self.s3_utils.s3.copy_object.return_value = None
        self.s3_utils.s3.delete_object.return_value = None

        # Llamar a la función que se está probando
        destination = self.s3_utils.move_file_to_rechazados('test-bucket', 'test-key')

        # Verificar que la ruta destino no sea None
        self.assertIsNotNone(destination)

        # Verificar la ruta destino y la llamada a S3
        self.assertTrue(destination.startswith(env.DIR_REJECTED_FILES))
        self.s3_utils.s3.copy_object.assert_called_once_with(
            Bucket='test-bucket',
            CopySource={'Bucket': 'test-bucket', 'Key': 'test-key'},
            Key=f"{env.DIR_REJECTED_FILES}/202411/test-key"
        )
        self.s3_utils.s3.delete_object.assert_called_once_with(
            Bucket='test-bucket',
            Key='test-key'
        )

    def test_move_file_to_procesando_success(self):
        file_name = 'test-file.txt'
        self.s3_utils.s3.copy_object.return_value = None
        self.s3_utils.s3.delete_object.return_value = None

        destination = self.s3_utils.move_file_to_procesando('test-bucket', file_name)

        # Verificar la ruta destino y la llamada a S3
        self.assertTrue(destination.startswith(env.DIR_PROCESSING_FILES))
        self.s3_utils.s3.copy_object.assert_called_once()
        self.s3_utils.s3.delete_object.assert_called_once()

    @patch('src.services.aws_clients_service.AWSClients.get_s3_client')
    def test_validate_decompressed_files_in_processing(self, mock_s3_client):
        # Simular el cliente S3
        mock_s3 = MagicMock()
        mock_s3_client.return_value = mock_s3

        # Configurar el retorno para list_objects_v2
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'processing_folder/test_file_/file1.txt'}
            ]
        }

        # Llamar a la función que se está probando
        result = self.s3_utils.validate_decompressed_files_in_processing(
            'test-bucket', 'processing_folder', 'test_file.zip'
        )

        # Verificar el resultado esperado
        self.assertFalse(result)


class TestS3UtilsZip(unittest.TestCase):
    @patch('boto3.client')
    @patch('src.services.aws_clients_service.AWSClients.get_ssm_client')
    @patch('src.services.aws_clients_service.AWSClients.get_s3_client')
    def setUp(self, mock_boto_client, mock_get_ssm_client, mock_get_s3_client):
        # Configurar el mock para el cliente S3 y SSM
        mock_ssm_client = MagicMock()
        mock_s3_client = MagicMock()

        mock_get_ssm_client.return_value = mock_ssm_client
        mock_get_s3_client.return_value = mock_s3_client

        mock_ssm_client.get_parameter.return_value = {
            'Parameter': {
                'Value': '{"special_start": "RE_ESP",'
                         ' "special_end": "0001", '
                         '"general_start": "RE_GEN", '
                         '"files-reponses-debito-reverso": "001,002", '
                         '"files-reponses-reintegros": "R", '
                         '"files-reponses-especiales": "ESP"}'
            }
        }

        self.s3_utils = S3Utils(MagicMock())
        self.error_handling_service = MagicMock(spec=ErrorHandlingService)

        # Mockear métodos de S3
        self.s3_utils.s3 = MagicMock()
        self.s3_utils.s3.get_object = MagicMock()
        self.s3_utils.s3.upload_fileobj = MagicMock()
        self.s3_utils.s3.delete_object = MagicMock()

        # Mockear métodos de validación y base de datos
        self.s3_utils.validator = MagicMock()
        self.s3_utils.get_cantidad_de_archivos_esperados_en_el_zip = MagicMock(return_value=(2, '01'))
        self.s3_utils.rta_procesamiento_repository.get_id_rta_procesamiento = MagicMock(return_value=123)

        # Crear un archivo zip válido en memoria
        self.mock_zip_content = BytesIO()
        with ZipFile(self.mock_zip_content, 'w') as zip_file:
            zip_file.writestr('file1-01.txt', 'Contenido del archivo 1')
            zip_file.writestr('file2-01.txt', 'Contenido del archivo 2')
        self.mock_zip_content.seek(0)

    def test_unzip_file_in_s3_success(self):
        # Configurar el mock para devolver un archivo zip válido
        self.s3_utils.s3.get_object.return_value = {'Body': self.mock_zip_content}
        self.s3_utils.validator.is_valid_extracted_filename = MagicMock(return_value=True)

        # Ejecutar la función
        self.s3_utils.unzip_file_in_s3(
            'test-bucket',
            'test-file.zip',
            1,
            'test-file',
            'receipt_handle',
            self.error_handling_service
        )

        # Verificar que se subieron los archivos extraídos a S3
        self.s3_utils.s3.upload_fileobj.assert_called()
        self.s3_utils.s3.delete_object.assert_called_once_with(Bucket='test-bucket', Key='test-file.zip')

    #
    def test_unzip_file_in_s3_bad_zip(self):
        # Configurar el mock para lanzar un error de archivo inválido
        self.s3_utils.s3.get_object.side_effect = BadZipFile

        # Ejecutar la función y verificar que se maneja el error
        with self.assertRaises(CustomFunctionError) as context:
            self.s3_utils.unzip_file_in_s3(
                'test-bucket',
                'bad-file.zip',
                1,
                'bad-file',
                'receipt_handle',
                self.error_handling_service
            )

        # Verificar que el error tenga el código correcto
        self.assertEqual(context.exception.code, env.CONST_COD_ERROR_CORRUPTED_FILE)
        self.assertEqual(context.exception.error_details, ".zip es inválido o está corrupto")
        self.assertFalse(context.exception.is_technical_error)

        # Verificar que se manejó el error adecuadamente
        self.error_handling_service.handle_generic_error.assert_called_once()

    def test_unzip_file_in_s3_invalid_filename_structure(self):
        # Configurar el mock para devolver un archivo zip válido con nombres incorrectos
        self.s3_utils.s3.get_object.return_value = {'Body': self.mock_zip_content}
        self.s3_utils.validator.is_valid_extracted_filename = MagicMock(return_value=False)

        # Ejecutar la función
        self.s3_utils.unzip_file_in_s3(
            'test-bucket',
            'test-file.zip',
            1,
            'test-file',
            'receipt_handle',
            self.error_handling_service
        )

    def test_unzip_file_in_s3_unexpected_file_count(self):
        # Configurar el mock para devolver un archivo zip con un número inesperado de archivos
        self.s3_utils.get_cantidad_de_archivos_esperados_en_el_zip.return_value = (3, '01')
        self.s3_utils.s3.get_object.return_value = {'Body': self.mock_zip_content}
        self.s3_utils.validator.is_valid_extracted_filename = MagicMock(return_value=True)

        # Ejecutar la función y verificar que se lanza la excepción
        with self.assertRaises(CustomFunctionError) as context:
            self.s3_utils.unzip_file_in_s3(
                'test-bucket',
                'test-file.zip',
                1,
                'test-file',
                'receipt_handle',
                self.error_handling_service
            )

        # Verificar que el error tenga el código correcto
        self.assertEqual(context.exception.code, env.CONST_COD_ERROR_UNEXPECTED_FILE_COUNT)
        self.assertEqual(context.exception.error_details,
                         "La cantidad de archivos descomprimidos no es igual a la cantidad esperada")
        self.assertFalse(context.exception.is_technical_error)

        # Verificar que se manejó el error de cantidad de archivos inesperada
        self.error_handling_service.handle_generic_error.assert_called_once()


class TestExtractAndValidateEventData(unittest.TestCase):
    def setUp(self):
        self.valid_event = {
            "Records": [
                {"body": json.dumps({"file_id": 123, "response_processing_id": 456})},
                {"body": json.dumps({"file_id": 789, "response_processing_id": 101})},
            ]
        }

        self.invalid_event = {
            "Records": [
                {"body": json.dumps({"file_id": 123})},  # Falta response_processing_id
                {"body": "INVALID_JSON"},  # Body mal formado
            ]
        }

        self.empty_event = {"Records": []}

    @patch("src.utils.logger_utils.logging.debug")
    @patch("src.utils.logger_utils.logging.error")
    def test_valid_event(self, mock_error, mock_warning):
        result = extract_and_validate_event_data(self.valid_event, required_keys=["file_id", "response_processing_id"])
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["file_id"], 123)
        self.assertEqual(result[0]["response_processing_id"], 456)
        self.assertEqual(result[1]["file_id"], 789)
        self.assertEqual(result[1]["response_processing_id"], 101)
        mock_error.assert_not_called()
        mock_warning.assert_not_called()

    @patch("src.utils.logger_utils.logging.warning")
    @patch("src.utils.logger_utils.logging.error")
    def test_empty_event(self, mock_error, mock_warning):
        result = extract_and_validate_event_data(self.empty_event, required_keys=["file_id", "response_processing_id"])
        self.assertEqual(result, [])
        mock_error.assert_not_called()
        mock_warning.assert_not_called()

    @patch("src.utils.logger_utils.logging.debug")
    @patch("src.utils.logger_utils.logging.error")
    def test_no_required_keys(self, mock_error, mock_warning):
        result = extract_and_validate_event_data(self.valid_event)
        self.assertEqual(len(result), 2)  # Sin claves requeridas, acepta todo
        mock_error.assert_not_called()
        mock_warning.assert_not_called()

    @patch("src.core.process_event.logger.debug")
    @patch("src.core.process_event.logger.error")
    def test_invalid_event(self, mock_warning, mock_error):
        result = extract_and_validate_event_data(
            self.invalid_event, required_keys=["file_id", "response_processing_id"]
        )
        self.assertEqual(result, [])
        mock_error.assert_called_once()
        mock_warning.assert_called_once()


class TestGetCantidadDeArchivosEsperadosEnElZip(unittest.TestCase):
    @patch('boto3.client')
    @patch('src.services.aws_clients_service.AWSClients.get_ssm_client')
    @patch('src.services.aws_clients_service.AWSClients.get_s3_client')
    def setUp(self, mock_boto_client, mock_get_ssm_client, mock_get_s3_client):
        # Configurar el mock para el cliente S3 y SSM
        mock_ssm_client = MagicMock()
        mock_s3_client = MagicMock()

        mock_get_ssm_client.return_value = mock_ssm_client
        mock_get_s3_client.return_value = mock_s3_client

        mock_ssm_client.get_parameter.return_value = {
            'Parameter': {
                'Value': '{"special_start": "RE_ESP",'
                         ' "special_end": "0001", '
                         '"general_start": "RE_GEN", '
                         '"files-reponses-debito-reverso": "001,002", '
                         '"files-reponses-reintegros": "R", '
                         '"files-reponses-especiales": "ESP"}'
            }
        }

        self.mock_db = MagicMock()
        self.service = s3_service.S3Utils(self.mock_db)
        self.service.rta_procesamiento_repository = MagicMock(spec=RtaProcesamientoRepository)

    def test_valid_tipo_respuesta_01(self):
        """Test para tipo_respuesta válido '01'."""
        self.service.rta_procesamiento_repository.get_tipo_respuesta.return_value = "01"
        id_archivo = 123
        nombre_archivo = "archivo_test.zip"

        expected_file_count, tipo_respuesta = self.service.get_cantidad_de_archivos_esperados_en_el_zip(
            id_archivo, nombre_archivo)

        self.assertEqual(expected_file_count, 5)
        self.assertEqual(tipo_respuesta, "01")
