import json
import unittest
from io import BytesIO

from src.config.config import env
from src.utils.event_utils import (
    extract_filename_from_body,
    extract_bucket_from_body,
    extract_date_from_filename,
    create_file_id,
    extract_consecutivo_plataforma_origen,
)
from unittest.mock import patch, MagicMock, create_autospec
from botocore.exceptions import ClientError
from src.utils.sqs_utils import delete_message_from_sqs, send_message_to_sqs, build_email_message
from src.utils.singleton import SingletonMeta
from src.utils.s3_utils import S3Utils


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

    @patch('src.utils.event_utils.env')
    def test_extract_date_from_filename_valid(self, mock_env):
        # Configurar el prefijo del archivo especial en la variable de entorno
        mock_env.CONST_PRE_SPECIAL_FILE = "RE_ESP"
        filename = "RE_ESP_TUTGMF0001003920241002-0001.zip"

        # Ejecutar y verificar que la fecha extraída es correcta
        result = extract_date_from_filename(filename)
        self.assertEqual(result, "20241002")

    @patch('src.utils.event_utils.env')
    def test_extract_date_from_filename_invalid(self, mock_env):
        # Configurar el prefijo del archivo especial en la variable de entorno
        mock_env.CONST_PRE_SPECIAL_FILE = "RE_ESP"
        filename = "RE_ESP_WRONG_FORMAT.zip"

        # Ejecutar y verificar que devuelve una cadena vacía para formato incorrecto
        result = extract_date_from_filename(filename)
        self.assertEqual(result, "")

    @patch('src.utils.event_utils.env')
    def test_create_file_id_with_valid_date(self, mock_env):
        # Configurar el prefijo del archivo especial en la variable de entorno
        mock_env.CONST_PRE_SPECIAL_FILE = "RE_ESP"
        filename = "RE_ESP_TUTGMF0001003920241002-0001.zip"

        # Ejecutar y verificar que el ID de archivo generado es correcto
        result = create_file_id(filename)
        self.assertEqual(result, 2024100201050001)

    @patch('src.utils.event_utils.env')
    def test_create_file_id_with_invalid_date(self, mock_env):
        # Configurar el prefijo del archivo especial en la variable de entorno
        mock_env.CONST_PRE_SPECIAL_FILE = "RE_ESP"
        filename = "RE_ESP_WRONG_FORMAT.zip"

        # Ejecutar y verificar que el ID de archivo devuelto sea None para un formato inválido
        result = create_file_id(filename)
        self.assertEqual(result, None)

    @patch('src.utils.event_utils.env')
    def test_extract_consecutivo_plataforma_origen(self, mock_env):
        # Configurar el prefijo del archivo especial en la variable de entorno
        mock_env.CONST_PRE_SPECIAL_FILE = "RE_ESP"
        filename = "RE_ESP_TUTGMF0001003920241002-0001.zip"

        # Ejecutar y verificar que el consecutivo extraído es correcto
        result = extract_consecutivo_plataforma_origen(filename)
        self.assertEqual(result, "0001")


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

    @patch('src.aws.clients.AWSClients.get_sqs_client')
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

    @patch('src.aws.clients.AWSClients.get_sqs_client')
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

    @patch('src.aws.clients.AWSClients.get_sqs_client')
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

    @patch('src.aws.clients.AWSClients.get_sqs_client')
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


import unittest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
from src.utils.s3_utils import S3Utils
from src.models.cgd_rta_procesamiento import CGDRtaProcesamiento
from sqlalchemy.orm import Session


class TestS3Utils(unittest.TestCase):
    def setUp(self):
        # Crear un mock de la sesión de la base de datos
        self.mock_db = MagicMock(spec=Session)
        self.s3_utils = S3Utils(db=self.mock_db)

        # Mock de cliente S3
        self.s3_utils.s3 = MagicMock()

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

    def test_move_file_to_rechazados_file_does_not_exist(self):
        # Simular que el archivo no existe
        self.s3_utils.check_file_exists_in_s3 = MagicMock(return_value=False)

        with self.assertRaises(SystemExit):
            self.s3_utils.move_file_to_rechazados('test-bucket', 'test-key')

    def test_move_file_to_rechazados_success(self):
        # Simular que el archivo existe
        self.s3_utils.check_file_exists_in_s3 = MagicMock(return_value=True)

        # Simular el movimiento de archivo
        self.s3_utils.s3.copy_object.return_value = None
        self.s3_utils.s3.delete_object.return_value = None

        destination = self.s3_utils.move_file_to_rechazados('test-bucket', 'test-key')

        self.assertTrue(destination.startswith(env.DIR_REJECTED_FILES))
        self.s3_utils.s3.copy_object.assert_called_once()
        self.s3_utils.s3.delete_object.assert_called_once()

    def test_move_file_to_procesando_success(self):
        file_name = 'test-file.txt'
        self.s3_utils.s3.copy_object.return_value = None
        self.s3_utils.s3.delete_object.return_value = None

        destination = self.s3_utils.move_file_to_procesando('test-bucket', file_name)

        self.assertTrue(destination.startswith(env.DIR_PROCESSING_FILES))
        self.s3_utils.s3.copy_object.assert_called_once()
        self.s3_utils.s3.delete_object.assert_called_once()

    def test_unzip_file_in_s3_success(self):
        # Simular obtener un objeto zip de S3
        mock_zip_content = MagicMock()
        self.s3_utils.s3.get_object.return_value = {
            'Body': BytesIO(b'Test zip content')
        }
        self.s3_utils.s3.upload_fileobj = MagicMock()

        # Simular que el método para obtener la cantidad de archivos esperados retorna un valor
        self.s3_utils.get_cantidad_de_archivos_esperados_en_el_zip = MagicMock(return_value=(2, '01'))

        # Simular validación
        self.s3_utils.validator.is_valid_extracted_filename = MagicMock(return_value=True)

        # Ejecutar el método
        self.s3_utils.unzip_file_in_s3(
            'test-bucket',
            'test-file.zip',
            1,
            'test-file',
            1,
            'receipt_handle',
            'error_handling_service'
        )

        # Verificaciones
        self.s3_utils.s3.delete_object.assert_called_once_with(Bucket='test-bucket', Key='test-file.zip')
        self.s3_utils.s3.upload_fileobj.assert_called()  # Verifica que se haya llamado al menos una vez

    def test_unzip_file_in_s3_bad_zip(self):
        self.s3_utils.s3.get_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "GetObject"
        )

        with self.assertRaises(SystemExit):
            self.s3_utils.unzip_file_in_s3('test-bucket', 'test-file.zip', 1, 'test-file', 1, 'receipt_handle',
                                           'error_handling_service')


if __name__ == '__main__':
    unittest.main()
