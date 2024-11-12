import json
import unittest
from fileinput import filename
from unittest.mock import patch, MagicMock

from src.config.config import env
from src.services.archivo_service import ArchivoService


class TestValidateEventData(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()
        # Mock para get_ssm_client para que devuelva un valor válido
        with patch("src.aws.clients.AWSClients.get_s3_client"), \
                patch("src.aws.clients.AWSClients.get_secrets_manager_client"), \
                patch("src.aws.clients.AWSClients.get_ssm_client") as mock_ssm_client:
            # Configurar el cliente SSM simulado
            mock_ssm = MagicMock()
            mock_ssm.get_parameter.return_value = {
                'Parameter': {'Value': '{"key": "value"}'}
            }
            mock_ssm_client.return_value = mock_ssm

            self.service = ArchivoService(self.mock_db)

    @patch("src.utils.sqs_utils.delete_message_from_sqs")
    @patch("src.logs.logger")
    def test_validate_event_data_valid_input(self, mock_logger, mock_delete_message_from_sqs):
        """
        Caso de prueba para validar el método validate_event_data con datos válidos.
        """
        # Caso en el que file_name y bucket_name están presentes
        file_name = "test_file.txt"
        bucket_name = "test_bucket"
        receipt_handle = "test_receipt_handle"

        # Llamada a la función y verificación de que devuelve True
        result = self.service.validate_event_data(file_name, bucket_name, receipt_handle)
        self.assertTrue(result)

        # Verificación de que se llamó a delete_message_from_sqs
        mock_delete_message_from_sqs.assert_not_called()

        # Verificación de que se llamó a logger.info
        mock_logger.info.assert_not_called()

    @patch("src.utils.sqs_utils.delete_message_from_sqs")
    @patch("src.logs.logger")
    def test_validate_event_data_invalid_input(self, mock_logger, mock_delete_message_from_sqs):
        """
        Caso de prueba para validar el método validate_event_data con datos inválidos.
        """
        # Caso en el que file_name y bucket_name están presentes

        # Llamada a la función y verificación de que devuelve False
        result = self.service.validate_event_data(None, None, None)
        self.assertFalse(result)

    @patch("src.utils.event_utils.extract_filename_from_body")
    @patch("src.utils.event_utils.extract_bucket_from_body")
    def test_extract_event_details_valid_event(self, mock_extract_bucket, mock_extract_filename):
        # Configurar el mock para las funciones de extracción
        mock_extract_filename.return_value = "test_file.txt"
        mock_extract_bucket.return_value = "test_bucket"

        # Simular un evento con datos válidos
        event = {
            "Records": [
                {
                    "receiptHandle": "test_receipt_handle",
                    "body": "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2020-09-25T15:43:27.121Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:EXAMPLE\"},\"requestParameters\":{\"sourceIPAddress\":\"205.255.255.255\"},\"responseElements\":{\"x-amz-request-id\":\"EXAMPLE123456789\",\"x-amz-id-2\":\"EXAMPLE123/5678ABCDEFGHIJK12345EXAMPLE=\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"testConfigRule\",\"bucket\":{\"name\":\"01-bucketrtaprocesa-d01\",\"ownerIdentity\":{\"principalId\":\"EXAMPLE\"},\"arn\":\"arn:aws:s3:::example-bucket\"},\"object\":{\"key\":\"Recibidos/RE_ESP_TUTGMF0001003920241002-0001.zip\",\"size\":1024,\"eTag\":\"0123456789abcdef0123456789abcdef\",\"sequencer\":\"0A1B2C3D4E5F678901\"}}}]}",

                }
            ]
        }

        # Crear instancia de la clase y llamar al método
        result = self.service.extract_event_details(event)

        # Verificar el resultado esperado
        expected_file_name = "RE_ESP_TUTGMF0001003920241002-0001.zip"
        expected_bucket_name = "01-bucketrtaprocesa-d01"
        expected_receipt_handle = "test_receipt_handle"
        expected_acg_nombre_archivo = "RE_ESP_TUTGMF0001003920241002-0001"  # El nombre sin la extensión

        self.assertEqual(result, (
            expected_file_name, expected_bucket_name, expected_receipt_handle, expected_acg_nombre_archivo))


class TestValidateFileExistenceInBucket(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()

        # Mock del cliente S3 y otros servicios de AWS
        with patch("src.aws.clients.AWSClients.get_s3_client"), \
                patch("src.aws.clients.AWSClients.get_secrets_manager_client"), \
                patch("src.aws.clients.AWSClients.get_ssm_client") as mock_ssm_client:
            # Configurar el mock del cliente SSM
            mock_ssm_instance = MagicMock()
            mock_ssm_client.return_value = mock_ssm_instance
            # Configura la respuesta del cliente SSM
            mock_ssm_instance.get_parameter.return_value = {
                'Parameter': {
                    'Value': '{"config_key": "config_value"}'
                }
            }

            self.service = ArchivoService(self.mock_db)

    @patch("src.utils.sqs_utils.delete_message_from_sqs")
    @patch("src.logs.logger")
    @patch("src.utils.s3_utils.S3Utils.check_file_exists_in_s3")
    def test_validate_file_existence_file_exists(self, mock_check_file_exists, mock_logger, mock_delete_message):
        """
        Caso en el que el archivo sí existe en el bucket.
        """
        # Configurar el mock para que devuelva True, indicando que el archivo existe
        mock_check_file_exists.return_value = True

        # Datos de entrada
        file_name = "test_file.txt"
        bucket_name = "test_bucket"
        receipt_handle = "test_receipt_handle"

        # Llamar a la función y verificar que devuelve True
        result = self.service.validate_file_existence_in_bucket(file_name, bucket_name, receipt_handle)
        self.assertTrue(result)

        # Verificar que no se llamó a delete_message_from_sqs ni al logger
        mock_delete_message.assert_not_called()
        mock_logger.error.assert_not_called()

    @patch("src.utils.sqs_utils.delete_message_from_sqs")
    @patch("src.logs.logger")
    @patch("src.utils.s3_utils.S3Utils.check_file_exists_in_s3")
    def test_validate_file_existence_file_not_exists(self, mock_check_file_exists, mock_logger, mock_delete_message):
        """
        Caso en el que el archivo no existe en el bucket.
        """
        # Configurar el mock para que devuelva False, indicando que el archivo no existe
        mock_check_file_exists.return_value = False

        # Datos de entrada
        file_name = "test_file.txt"
        bucket_name = "test_bucket"
        receipt_handle = "test_receipt_handle"
        file_key = f"{env.DIR_RECEPTION_FILES}/{file_name}"

        # Llamar a la función y verificar que devuelve False
        result = self.service.validate_file_existence_in_bucket(file_name, bucket_name, receipt_handle)
        self.assertFalse(result)

        #


class TestProcessSpecialFile(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()

        # Moquear los clientes de AWS
        with patch("src.aws.clients.AWSClients.get_s3_client"), \
                patch("src.aws.clients.AWSClients.get_secrets_manager_client"), \
                patch("src.aws.clients.AWSClients.get_ssm_client") as mock_ssm_client:
            # Configura el mock para el cliente SSM
            mock_ssm = MagicMock()
            mock_ssm.get_parameter.return_value = {
                'Parameter': {
                    'Value': '{"config_key": "config_value"}'
                }
            }
            mock_ssm_client.return_value = mock_ssm

            # Inicializar el servicio con el mock
            self.service = ArchivoService(self.mock_db)

        # Moquear los métodos utilizados en ArchivoService
        self.service.archivo_repository = MagicMock()
        self.service.archivo_validator = MagicMock()
        self.service.error_handling_service = MagicMock()

        self.service.check_existing_special_file = MagicMock()
        self.service.validar_estado_special_file = MagicMock()
        self.service.move_file_and_update_state = MagicMock()
        self.service.insert_file_states_and_rta_processing = MagicMock()
        self.service.unzip_file = MagicMock()
        self.service.process_sqs_response = MagicMock()
        self.service.create_and_process_new_special_file = MagicMock()
        self.service.handle_invalid_special_file = MagicMock()

    @patch("src.logs.logger")
    def test_process_special_file_existing_special_file(self, mock_logger):
        """
        Caso en el que el archivo es especial y ya existe.
        """
        # Configurar los mocks
        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.id_archivo = 123
        self.service.archivo_validator.is_special_file.return_value = True
        self.service.check_existing_special_file.return_value = True
        self.service.validar_estado_special_file.return_value = True

        # Datos de entrada
        file_name = "RE_ESP_FILE.txt"
        bucket = "test_bucket"
        receipt_handle = "test_receipt_handle"
        acg_nombre_archivo = "RE_ESP_FILE"

        # Llamar a la función
        self.service.process_special_file(file_name, bucket, receipt_handle, acg_nombre_archivo)

        # Verificar que se llamó a las funciones esperadas
        self.service.move_file_and_update_state.assert_called_once_with(bucket, file_name, acg_nombre_archivo)
        self.service.insert_file_states_and_rta_processing.assert_called_once_with(acg_nombre_archivo, True, file_name)
        self.service.unzip_file.assert_called_once()
        self.service.process_sqs_response.assert_called_once()

    class TestProcessSpecialFile(unittest.TestCase):
        def __init__(self, methodName: str = "runTest"):
            super().__init__(methodName)
            self.service = None

        @patch(
            "src.services.archivo_service.ArchivoService.insertar_archivo_nuevo_especial")
        @patch("src.services.archivo_service.ArchivoService.archivo_repository")
        @patch("src.services.archivo_service.ArchivoService.archivo_validator")
        @patch("src.services.archivo_service.logger")
        def test_process_special_file_new_special_file(
                self,
                mock_logger,
                mock_archivo_validator,
                mock_archivo_repository,
                mock_insertar_archivo_nuevo_especial):
            """
            Caso en el que el archivo es especial, pero no existe previamente.
            """
            # Configurar los mocks
            mock_archivo_validator.is_special_file.return_value = True
            mock_archivo_repository.get_archivo_by_nombre_archivo.return_value.id_archivo = 123
            self.service.check_existing_special_file.return_value = False

            # Datos de entrada
            file_name = "RE_ESP_FILE.txt"
            bucket = "test_bucket"
            receipt_handle = "test_receipt_handle"
            acg_nombre_archivo = "RE_ESP_FILE"
            file_key = f"{env.DIR_RECEPTION_FILES}/{file_name}"

            # Llamar a la función
            self.service.process_special_file(file_name, bucket, receipt_handle, acg_nombre_archivo)

            # Verificar que se llamó a insertar_archivo_nuevo_especial con los argumentos correctos
            mock_insertar_archivo_nuevo_especial.assert_called_once_with(
                bucket,
                file_key,
                file_name,
                acg_nombre_archivo,
                receipt_handle
            )

            # Verificar que logger.debug fue llamado para el mensaje correspondiente
            mock_logger.debug.assert_called_with(f"El archivo especial {file_name} no existe en la base de datos.")

    @patch("src.logs.logger")
    def test_process_special_file_invalid_file(self, mock_logger):
        """
        Caso en el que el archivo no es especial.
        """
        # Configurar los mocks
        self.service.archivo_validator.is_special_file.return_value = False

        # Datos de entrada
        file_name = "INVALID_FILE.txt"
        bucket = "test_bucket"
        receipt_handle = "test_receipt_handle"
        acg_nombre_archivo = "INVALID_FILE"

        # Llamar a la función
        self.service.process_special_file(file_name, bucket, receipt_handle, acg_nombre_archivo)

        # Verificar que se manejó como archivo no válido
        self.service.handle_invalid_special_file.assert_called_once_with(file_name, bucket, receipt_handle)


class TestCheckExistingSpecialFile(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()

        # Moquear los clientes de AWS
        with patch("src.aws.clients.AWSClients.get_s3_client"), \
                patch("src.aws.clients.AWSClients.get_secrets_manager_client"), \
                patch("src.aws.clients.AWSClients.get_ssm_client") as mock_ssm_client:
            # Configura el mock para el cliente SSM
            mock_ssm = MagicMock()
            mock_ssm.get_parameter.return_value = {
                'Parameter': {
                    'Value': '{"config_key": "config_value"}'
                }
            }
            mock_ssm_client.return_value = mock_ssm

            # Inicializar el servicio con el mock
            self.service = ArchivoService(self.mock_db)

        # Moquear los métodos utilizados en ArchivoService
        self.service.archivo_repository = MagicMock()

    @patch("src.logs.logger")
    def test_check_existing_special_file_exists(self, mock_logger):
        """
        Caso en el que el archivo especial ya existe en la base de datos.
        """
        # Configurar el mock para que devuelva True, indicando que el archivo existe
        self.service.archivo_repository.check_special_file_exists.return_value = True

        # Datos de entrada
        acg_nombre_archivo = "RE_ESP_FILE"

        # Llamar a la función
        result = self.service.check_existing_special_file(acg_nombre_archivo)

        # Verificar que la función devuelve True
        self.assertTrue(result)

    @patch("src.logs.logger")
    def test_check_existing_special_file_not_exists(self, mock_logger):
        """
        Caso en el que el archivo especial no existe en la base de datos.
        """
        # Configurar el mock para que devuelva False, indicando que el archivo no existe
        self.service.archivo_repository.check_special_file_exists.return_value = False

        # Datos de entrada
        acg_nombre_archivo = "RE_ESP_FILE"

        # Llamar a la función
        result = self.service.check_existing_special_file(acg_nombre_archivo)

        # Verificar que la función devuelve False
        self.assertFalse(result)

        # Verificar que no se registró ningún mensaje de advertencia
        mock_logger.warning.assert_not_called()


class TestValidarEstadoSpecialFile(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()

        # Moquear los clientes de AWS
        with patch("src.aws.clients.AWSClients.get_s3_client"), \
                patch("src.aws.clients.AWSClients.get_secrets_manager_client"), \
                patch("src.aws.clients.AWSClients.get_ssm_client") as mock_ssm_client:
            # Configurar el mock para el cliente SSM
            mock_ssm = MagicMock()
            mock_ssm.get_parameter.return_value = {
                'Parameter': {
                    'Value': '{"config_key": "config_value"}'
                }
            }
            mock_ssm_client.return_value = mock_ssm

            # Inicializar el servicio con el mock de la base de datos
            self.service = ArchivoService(self.mock_db)

        # Moquear los métodos en ArchivoService
        self.service.archivo_repository = MagicMock()
        self.service.archivo_validator = MagicMock()
        self.service.error_handling_service = MagicMock()

    @patch("src.logs.logger")
    def test_estado_es_valido(self, mock_logger):
        """
        Caso en el que el estado del archivo es válido.
        """
        # Configurar el mock para que devuelva un estado válido
        estado_valido = "EN_PROCESO"
        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.estado = estado_valido
        self.service.archivo_validator.is_valid_state.return_value = True

        # Datos de entrada
        acg_nombre_archivo = "RE_ESP_FILE"
        bucket = "test_bucket"
        receipt_handle = "test_receipt_handle"

        # Llamar a la función
        result = self.service.validar_estado_special_file(acg_nombre_archivo, bucket, receipt_handle)

        # Verificar que la función devuelve el estado
        self.assertEqual(result, estado_valido)

    @patch("src.logs.logger")
    def test_estado_no_es_valido(self, mock_logger):
        """
        Caso en el que el estado del archivo no es válido.
        """
        # Configurar el mock para que devuelva un estado no válido
        estado_invalido = "INVALIDO"
        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.estado = estado_invalido
        self.service.archivo_validator.is_valid_state.return_value = False

        # Datos de entrada
        acg_nombre_archivo = "RE_ESP_FILE"
        bucket = "test_bucket"
        receipt_handle = "test_receipt_handle"

        # Llamar a la función
        result = self.service.validar_estado_special_file(acg_nombre_archivo, bucket, receipt_handle)

        # Verificar que la función no devuelve nada
        self.assertIsNone(result)

        # Verificar que se llamó al servicio de manejo de errores
        self.service.error_handling_service.handle_file_error.assert_called_once()

    @patch("src.logs.logger")
    @patch("sys.exit")
    def test_archivo_sin_estado(self, mock_exit, mock_logger):
        """
        Caso en el que el archivo no tiene estado.
        """
        # Configurar el mock para que devuelva None como estado
        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.estado = None

        # Datos de entrada
        acg_nombre_archivo = "RE_ESP_FILE"
        bucket = "test_bucket"
        receipt_handle = "test_receipt_handle"

        # Llamar a la función
        self.service.validar_estado_special_file(acg_nombre_archivo, bucket, receipt_handle)

        # Verificar que se llama a sys.exit(1)
        mock_exit.assert_called_once_with(1)


class TestGetEstadoArchivo(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()

        # Moquear los clientes de AWS
        with patch("src.aws.clients.AWSClients.get_s3_client"), \
                patch("src.aws.clients.AWSClients.get_secrets_manager_client"), \
                patch("src.aws.clients.AWSClients.get_ssm_client") as mock_ssm_client:
            # Configurar el mock para el cliente SSM
            mock_ssm = MagicMock()
            mock_ssm.get_parameter.return_value = {
                'Parameter': {
                    'Value': '{"config_key": "config_value"}'
                }
            }
            mock_ssm_client.return_value = mock_ssm

            # Inicializar el servicio con el mock de la base de datos
            self.service = ArchivoService(self.mock_db)

        # Moquear el archivo repository
        self.service.archivo_repository = MagicMock()

    @patch("src.logs.logger")
    def test_get_estado_archivo_con_estado(self, mock_logger):
        """
        Caso en el que el archivo tiene un estado.
        """
        # Configurar el mock para que devuelva un estado válido
        estado_esperado = "EN_PROCESO"
        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.estado = estado_esperado

        # Datos de entrada
        acg_nombre_archivo = "RE_ESP_FILE"

        # Llamar a la función
        result = self.service.get_estado_archivo(acg_nombre_archivo)

        # Verificar que la función devuelve el estado correcto
        self.assertEqual(result, estado_esperado)

        # Verificar que se registró un mensaje de depuración

    @patch("src.logs.logger")
    def test_get_estado_archivo_sin_estado(self, mock_logger):
        """
        Caso en el que el archivo no tiene un estado.
        """
        # Configurar el mock para que devuelva None como estado
        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.estado = None

        # Datos de entrada
        acg_nombre_archivo = "RE_ESP_FILE"

        # Llamar a la función
        result = self.service.get_estado_archivo(acg_nombre_archivo)

        # Verificar que la función devuelve None
        self.assertIsNone(result)

        # Verificar que se registró un mensaje de depuración


class TestMoveFileAndUpdateState(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()

        # Moquear los clientes de AWS
        with patch("src.aws.clients.AWSClients.get_s3_client"), \
                patch("src.aws.clients.AWSClients.get_secrets_manager_client"), \
                patch("src.aws.clients.AWSClients.get_ssm_client") as mock_ssm_client:
            # Configurar el mock para que devuelva un parámetro válido
            mock_ssm_client().get_parameter.return_value = {
                'Parameter': {
                    'Value': json.dumps({
                        'key1': 'value1',
                        'key2': 'value2'
                    })
                }
            }

            # Inicializar el servicio
            self.service = ArchivoService(self.mock_db)

        # Moquear s3_utils y archivo_repository
        self.service.s3_utils = MagicMock()
        self.service.archivo_repository = MagicMock()

    @patch("src.logs.logger")
    def test_move_file_and_update_state(self, mock_logger):
        """
        Caso en el que se mueve el archivo y se actualiza su estado correctamente.
        """
        # Configurar el mock para que devuelva una nueva ruta del archivo
        new_file_key = "procesando/new_file.txt"
        self.service.s3_utils.move_file_to_procesando.return_value = new_file_key

        # Datos de entrada
        bucket = "test_bucket"
        file_name = "test_file.txt"
        acg_nombre_archivo = "RE_ESP_FILE"

        # Llamar a la función
        result = self.service.move_file_and_update_state(bucket, file_name, acg_nombre_archivo)

        # Verificar que la función devuelve la nueva ruta del archivo
        self.assertEqual(result, new_file_key)

        # Verificar que se llamó a move_file_to_procesando con los argumentos correctos
        self.service.s3_utils.move_file_to_procesando.assert_called_once_with(bucket, file_name)

        # Verificar que se actualizó el estado en la base de datos
        self.service.archivo_repository.update_estado_archivo.assert_called_once_with(
            acg_nombre_archivo, env.CONST_ESTADO_LOAD_RTA_PROCESSING, 0
        )


class TestInsertFileStates(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock()

        # Moquear los clientes de AWS y sus respuestas
        with patch("src.aws.clients.AWSClients.get_s3_client"), \
                patch("src.aws.clients.AWSClients.get_secrets_manager_client"), \
                patch("src.aws.clients.AWSClients.get_ssm_client") as mock_ssm_client:
            # Configurar el mock para que devuelva un parámetro válido
            mock_ssm_client().get_parameter.return_value = {
                'Parameter': {
                    'Value': '{"key1": "value1", "key2": "value2"}'
                }
            }

            # Inicializar el servicio
            self.service = ArchivoService(self.mock_db)

        # Moquear los repositorios y validadores
        self.service.archivo_repository = MagicMock()
        self.service.estado_archivo_repository = MagicMock()
        self.service.rta_procesamiento_repository = MagicMock()
        self.service.archivo_validator = MagicMock()

    @patch("src.logs.logger")
    def test_insert_file_states(self, mock_logger):
        """
        Caso en el que se insertan correctamente los estados del archivo en la base de datos.
        """
        # Configurar los mocks para devolver valores simulados
        archivo_id = 123
        fecha_recepcion = "2024-11-07"
        contador_intentos = 2
        type_response = "01"

        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.id_archivo = archivo_id
        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.fecha_recepcion = fecha_recepcion
        self.service.rta_procesamiento_repository.get_last_contador_intentos_cargue.return_value = contador_intentos
        self.service.archivo_validator.get_type_response.return_value = type_response

        # Datos de entrada
        acg_nombre_archivo = "RE_ESP_FILE"
        estado = "EN_PROCESO"
        file_name = "RE_ESP_FILE.zip"

        # Simular que get_last_rta_procesamiento devuelve el último id_rta_procesamiento
        self.service.rta_procesamiento_repository.get_last_rta_procesamiento.return_value = MagicMock(
            id_rta_procesamiento=1)

        # Llamar a la función
        self.service.insert_file_states_and_rta_processing(acg_nombre_archivo, estado, file_name)

        # Verificar que se llamó a get_archivo_by_nombre_archivo correctamente
        self.service.archivo_repository.get_archivo_by_nombre_archivo.assert_called_with(acg_nombre_archivo)

        # Verificar que se insertó en CGD_ARCHIVO_ESTADOS
        self.service.estado_archivo_repository.insert_estado_archivo.assert_called_once_with(
            id_archivo=archivo_id,
            estado_inicial=estado,
            estado_final=env.CONST_ESTADO_LOAD_RTA_PROCESSING,
            fecha_cambio_estado=fecha_recepcion
        )

        # Verificar que se insertó en CGD_RTA_PROCESAMIENTO
        self.service.rta_procesamiento_repository.insert_rta_procesamiento.assert_called_once_with(
            id_archivo=archivo_id,
            id_rta_procesamiento=2,
            nombre_archivo_zip=file_name,
            tipo_respuesta=type_response,
            estado=env.CONST_ESTADO_INICIADO,
            contador_intentos_cargue=contador_intentos + 1
        )


class TestUnzipFile(unittest.TestCase):
    @patch("src.aws.clients.AWSClients.get_ssm_client")
    def setUp(self, mock_ssm_client):
        self.mock_db = MagicMock()

        # Configurar el cliente SSM mockeado para devolver una respuesta válida
        mock_ssm_client_instance = mock_ssm_client.return_value
        mock_ssm_client_instance.get_parameter.return_value = {
            'Parameter': {
                'Value': '{"CONFIG_NAME": "some_value"}'
            }
        }

        # Inicializar el servicio ArchivoService con el cliente mockeado
        self.service = ArchivoService(self.mock_db)
        self.service.s3_utils = MagicMock()

    @patch("src.logs.logger")
    def test_unzip_file(self, mock_logger):
        """
        Caso en el que se descomprime el archivo correctamente en S3.
        """
        # Datos de entrada
        bucket = "test_bucket"
        new_file_key = "procesando/new_file.zip"
        archivo_id = 123
        acg_nombre_archivo = "RE_ESP_FILE"
        new_counter = 0
        receipt_handle = "test_receipt_handle"
        error_handling_service = MagicMock()

        # Llamar a la función
        self.service.unzip_file(
            bucket,
            new_file_key,
            archivo_id,
            acg_nombre_archivo,
            new_counter,
            receipt_handle,
            error_handling_service,
        )

        # Verificar que se llamó a unzip_file_in_s3 con los argumentos correctos
        self.service.s3_utils.unzip_file_in_s3.assert_called_once_with(
            bucket,
            new_file_key,
            int(archivo_id),
            acg_nombre_archivo,
            new_counter,
            receipt_handle,
            error_handling_service,
        )

        # Verificar que no se registraron mensajes de error
        mock_logger.error.assert_not_called()


class TestProcessSqsResponse(unittest.TestCase):
    @patch("src.aws.clients.AWSClients.get_ssm_client")
    def setUp(self, mock_ssm_client):
        # Configurar el cliente SSM simulado para devolver una respuesta válida
        mock_ssm_client_instance = mock_ssm_client.return_value
        mock_ssm_client_instance.get_parameter.return_value = {
            'Parameter': {
                'Value': '{"CONFIG_NAME": "some_value"}'
            }
        }

        self.mock_db = MagicMock()
        self.service = ArchivoService(self.mock_db)
        self.service.rta_procesamiento_repository = MagicMock()

    @patch("src.utils.sqs_utils.delete_message_from_sqs")
    @patch("src.utils.sqs_utils.send_message_to_sqs")
    @patch("src.logs.logger")
    def test_process_sqs_response_estado_enviado(self, mock_logger, mock_send_message, mock_delete_message):
        """
        Caso en el que el estado ya está marcado como enviado.
        """
        # Configurar el mock para que `is_estado_enviado` devuelva True
        self.service.rta_procesamiento_repository.is_estado_enviado.return_value = True

        # Datos de entrada
        archivo_id = 123
        file_name = "test_file.txt"
        receipt_handle = "test_receipt_handle"

        # Llamar a la función
        self.service.process_sqs_response(archivo_id, file_name, receipt_handle)

    @patch("src.utils.sqs_utils.delete_message_from_sqs")
    @patch("src.utils.sqs_utils.send_message_to_sqs")
    @patch("src.logs.logger")
    def test_process_sqs_response_estado_no_enviado(self, mock_logger, mock_send_message, mock_delete_message):
        """
        Caso en el que el estado no está marcado como enviado.
        """
        # Configurar los mocks
        self.service.rta_procesamiento_repository.is_estado_enviado.return_value = False
        self.service.rta_procesamiento_repository.get_id_rta_procesamiento_by_id_archivo.return_value = 456

        # Datos de entrada
        archivo_id = 123
        file_name = "test_file.txt"
        receipt_handle = "test_receipt_handle"

        # Llamar a la función
        self.service.process_sqs_response(archivo_id, file_name, receipt_handle)

        # Verificar que se actualizó el estado a "enviado"
        self.service.rta_procesamiento_repository.update_state_rta_procesamiento.assert_called_once_with(
            id_archivo=archivo_id, estado=env.CONST_ESTADO_SEND
        )


class TestHandleInvalidSpecialFile(unittest.TestCase):
    @patch("src.aws.clients.AWSClients.get_ssm_client")
    def setUp(self, mock_ssm_client):
        # Configurar el cliente SSM simulado para devolver una respuesta válida
        mock_ssm_client_instance = mock_ssm_client.return_value
        mock_ssm_client_instance.get_parameter.return_value = {
            'Parameter': {
                'Value': '{"CONFIG_NAME": "some_value"}'
            }
        }

        self.mock_db = MagicMock()
        self.service = ArchivoService(self.mock_db)
        self.service.error_handling_service = MagicMock()

    @patch("src.logs.logger")
    def test_handle_invalid_special_file(self, mock_logger):
        """
        Caso en el que se maneja un archivo especial con formato incorrecto.
        """
        # Datos de entrada
        file_name = "invalid_file.txt"
        bucket = "test_bucket"
        receipt_handle = "test_receipt_handle"
        file_key = f"{env.DIR_RECEPTION_FILES}/{file_name}"

        # Llamar a la función
        self.service.handle_invalid_special_file(file_name, bucket, receipt_handle)

        # Verificar que se llamó a handle_file_error con los argumentos correctos
        self.service.error_handling_service.handle_file_error.assert_called_once_with(
            id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
            filekey=file_key,
            bucket=bucket,
            receipt_handle=receipt_handle,
            codigo_error=env.CONST_COD_ERROR_EMAIL,
            filename=file_name,
        )


class TestProcessGeneralFile(unittest.TestCase):
    @patch("src.aws.clients.AWSClients.get_ssm_client")
    def setUp(self, mock_ssm_client):
        # Configurar el cliente SSM simulado para devolver una respuesta válida
        mock_ssm_client_instance = mock_ssm_client.return_value
        mock_ssm_client_instance.get_parameter.return_value = {
            'Parameter': {
                'Value': '{"CONFIG_NAME": "some_value"}'
            }
        }

        self.mock_db = MagicMock()
        self.service = ArchivoService(self.mock_db)
        self.service.archivo_repository = MagicMock()
        self.service.archivo_validator = MagicMock()
        self.service.error_handling_service = MagicMock()
        self.service.s3_utils = MagicMock()

    @patch("src.logs.logger")
    def test_process_general_file_no_existe(self, mock_logger):
        """
        Caso en el que el archivo no existe en la base de datos.
        """
        # Configurar el mock para que el archivo no exista
        self.service.archivo_repository.check_file_exists.return_value = False

        # Datos de entrada
        file_name = "general_file.txt"
        bucket = "test_bucket"
        receipt_handle = "test_receipt_handle"
        acg_nombre_archivo = "GENERAL_FILE"

        # Llamar a la función
        self.service.process_general_file(file_name, bucket, receipt_handle, acg_nombre_archivo)

        # Verificar que se llamó a handle_file_error
        self.service.error_handling_service.handle_file_error.assert_called_once_with(
            id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
            filekey=f"{env.DIR_RECEPTION_FILES}/{file_name}",
            bucket=bucket,
            receipt_handle=receipt_handle,
            codigo_error=env.CONST_COD_ERROR_EMAIL,
            filename=file_name,
        )

    @patch("src.logs.logger")
    def test_process_general_file_estado_invalido(self, mock_logger):
        """
        Caso en el que el archivo tiene un estado no válido.
        """
        # Configurar los mocks
        self.service.archivo_repository.check_file_exists.return_value = True
        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.estado = "INVALIDO"
        self.service.archivo_validator.is_valid_state.return_value = False

        # Datos de entrada
        file_name = "general_file.txt"
        bucket = "test_bucket"
        receipt_handle = "test_receipt_handle"
        acg_nombre_archivo = "GENERAL_FILE"

        # Llamar a la función
        self.service.process_general_file(file_name, bucket, receipt_handle, acg_nombre_archivo)

        # Verificar que se llamó a handle_file_error
        self.service.error_handling_service.handle_file_error.assert_called_once_with(
            id_plantilla=env.CONST_ID_PLANTILLA_EMAIL,
            filekey=f"{env.DIR_RECEPTION_FILES}/{file_name}",
            bucket=bucket,
            receipt_handle=receipt_handle,
            codigo_error=env.CONST_COD_ERROR_EMAIL,
            filename=file_name,
        )

    @patch("src.logs.logger")
    def test_process_general_file_estado_valido(self, mock_logger):
        """
        Caso en el que el archivo existe y tiene un estado válido.
        """
        # Configurar los mocks para que el archivo exista y el estado sea válido
        self.service.archivo_repository.check_file_exists.return_value = True
        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.estado = "VALIDO"
        self.service.archivo_validator.is_valid_state.return_value = True

        # Datos de entrada
        file_name = "general_file.txt"
        bucket = "test_bucket"
        receipt_handle = "test_receipt_handle"
        acg_nombre_archivo = "GENERAL_FILE"

        # Llamar a la función
        self.service.process_general_file(file_name, bucket, receipt_handle, acg_nombre_archivo)

        # Verificar que se llamó a move_file_to_procesando
        self.service.s3_utils.move_file_to_procesando.assert_called_once_with(bucket, file_name)

        # Verificar que se actualizó el estado en la base de datos
        self.service.archivo_repository.update_estado_archivo.assert_called_once_with(
            acg_nombre_archivo, env.CONST_ESTADO_LOAD_RTA_PROCESSING, 0
        )


class TestValidarYProcesarArchivo(unittest.TestCase):
    @patch("src.aws.clients.AWSClients.get_ssm_client")
    def setUp(self, mock_ssm_client):
        # Configurar el cliente SSM simulado
        mock_ssm_instance = mock_ssm_client.return_value
        mock_ssm_instance.get_parameter.return_value = {
            'Parameter': {
                'Value': '{"CONFIG_NAME": "some_value"}'
            }
        }

        self.mock_db = MagicMock()
        self.service = ArchivoService(self.mock_db)
        self.service.archivo_validator = MagicMock()
        self.service.s3_utils = MagicMock()
        self.service.error_handling_service = MagicMock()
        self.service.validate_event_data = MagicMock()
        self.service.validate_file_existence_in_bucket = MagicMock()

    @patch("src.services.archivo_service.ArchivoService.extract_event_details")
    @patch("src.logs.logger")
    def test_event_data_no_valido(self, mock_logger, mock_extract_event_details):
        """
        Caso en el que los datos del evento no son válidos.
        """
        # Configurar el mock para `extract_event_details`
        mock_extract_event_details.return_value = (None, None, None, None)
        self.service.validate_event_data.return_value = False

        event = {"Records": []}

        # Llamar a la función
        self.service.validar_y_procesar_archivo(event)

    @patch("src.services.archivo_service.ArchivoService.extract_event_details")
    @patch("src.logs.logger")
    def test_archivo_no_existe_en_bucket(self, mock_logger, mock_extract_event_details):
        """
        Caso en el que el archivo no existe en el bucket.
        """
        # Configurar los mocks
        mock_extract_event_details.return_value = ("file.txt", "test_bucket", "receipt_handle", "ARCHIVO")
        self.service.validate_event_data.return_value = True
        self.service.validate_file_existence_in_bucket.return_value = False

        event = {"Records": []}

        # Llamar a la función
        self.service.validar_y_procesar_archivo(event)

        # Verificar que se llamó a validate_event_data y validate_file_existence_in_bucket
        self.service.validate_event_data.assert_called_once()
        self.service.validate_file_existence_in_bucket.assert_called_once_with("file.txt", "test_bucket",
                                                                               "receipt_handle")

    @patch("src.services.archivo_service.ArchivoService.extract_event_details")
    @patch("src.logs.logger")
    def test_archivo_especial(self, mock_logger, mock_extract_event_details):
        """
        Caso en el que el archivo tiene un prefijo especial.
        """
        # Configurar los mocks
        mock_extract_event_details.return_value = ("special_file.txt", "test_bucket", "receipt_handle", "ARCHIVO")
        self.service.validate_event_data.return_value = True
        self.service.validate_file_existence_in_bucket.return_value = True
        self.service.archivo_validator.is_special_prefix.return_value = True

        event = {"Records": []}

        # Llamar a la función
        self.service.validar_y_procesar_archivo(event)

    @patch("src.services.archivo_service.ArchivoService.extract_event_details")
    @patch("src.logs.logger")
    def test_archivo_general(self, mock_logger, mock_extract_event_details):
        """
        Caso en el que el archivo es general.
        """
        # Configurar los mocks
        mock_extract_event_details.return_value = ("general_file.txt", "test_bucket", "receipt_handle", "ARCHIVO")
        self.service.validate_event_data.return_value = True
        self.service.validate_file_existence_in_bucket.return_value = True
        self.service.archivo_validator.is_special_prefix.return_value = False

        event = {"Records": []}

        # Llamar a la función
        self.service.validar_y_procesar_archivo(event)

#
