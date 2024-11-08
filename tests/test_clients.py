import json
import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from src.aws.clients import AWSClients


class TestAWSClients(unittest.TestCase):

    @patch('boto3.client')
    @patch.dict('os.environ', {'APP_ENV': 'local'})
    def test_get_secrets_manager_client(self, mock_boto_client):
        client = AWSClients.get_secrets_manager_client()
        mock_boto_client.assert_called_once_with('secretsmanager', region_name='us-east-1',
                                                 endpoint_url='http://localhost:4566')
        self.assertEqual(client, mock_boto_client.return_value)

    @patch('boto3.client')
    @patch.dict('os.environ', {'APP_ENV': 'local'})
    def test_get_s3_client(self, mock_boto_client):
        client = AWSClients.get_s3_client()
        mock_boto_client.assert_called_once_with('s3', region_name='us-east-1', endpoint_url='http://localhost:4566')
        self.assertEqual(client, mock_boto_client.return_value)

    @patch('boto3.client')
    @patch.dict('os.environ', {'APP_ENV': 'local'})
    def test_get_sqs_client(self, mock_boto_client):
        client = AWSClients.get_sqs_client()
        mock_boto_client.assert_called_once_with('sqs', region_name='us-east-1', endpoint_url='http://localhost:4566')
        self.assertEqual(client, mock_boto_client.return_value)

    @patch('src.aws.clients.boto3.client')
    def test_get_ssm_client(self, mock_boto_client):
        # Crear un mock para el cliente de SSM
        mock_ssm_client = "mocked_ssm_client"  # Usar un valor de retorno simulado
        mock_boto_client.return_value = mock_ssm_client

        # Llamar al método que estás probando
        ssm_client = AWSClients.get_ssm_client()

        # Comprobar que el cliente SSM se ha creado correctamente
        self.assertEqual(ssm_client, mock_ssm_client)  # Comprobar el valor devuelto por el método

        # Test para la función get_secret
        @patch('src.aws.clients.AWSClients.get_secrets_manager_client')
        def test_get_secret_success(self, mock_get_secrets_manager_client):
            # Configuración del cliente simulado
            mock_client = MagicMock()
            mock_get_secrets_manager_client.return_value = mock_client
            mock_client.get_secret_value.return_value = {
                "SecretString": json.dumps({"username": "test_user", "password": "test_pass"})
            }

            # Llamada a la función
            secret = AWSClients.get_secret('test_secret')

            # Afirmaciones
            self.assertEqual(secret, {"username": "test_user", "password": "test_pass"})
            mock_client.get_secret_value.assert_called_once_with(SecretId='test_secret')

    @patch('src.aws.clients.AWSClients.get_secrets_manager_client')
    def test_get_secret_client_error(self, mock_get_secrets_manager_client):
        # Configuración del cliente simulado
        mock_client = MagicMock()
        mock_get_secrets_manager_client.return_value = mock_client
        mock_client.get_secret_value.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "Secret not found."}},
            operation_name='GetSecretValue'
        )

        # Llamada a la función
        secret = AWSClients.get_secret('test_secret')

        # Afirmaciones
        self.assertEqual(secret, {})
        mock_client.get_secret_value.assert_called_once_with(SecretId='test_secret')

        # Test para la función get_parameter

    @patch('src.aws.clients.AWSClients.get_ssm_client')
    def test_get_parameter_success(self, mock_get_ssm_client):
        # Configuración del cliente simulado
        mock_client = MagicMock()
        mock_get_ssm_client.return_value = mock_client
        mock_client.get_parameter.return_value = {
            "Parameter": {"Value": "test_value"}
        }

        # Llamada a la función
        parameter = AWSClients.get_parameter('test_parameter')

        # Afirmaciones
        self.assertEqual(parameter, "test_value")
        mock_client.get_parameter.assert_called_once_with(Name='test_parameter', WithDecryption=False)

    @patch('src.aws.clients.AWSClients.get_ssm_client')
    def test_get_parameter_client_error(self, mock_get_ssm_client):
        # Configuración del cliente simulado
        mock_client = MagicMock()
        mock_get_ssm_client.return_value = mock_client
        mock_client.get_parameter.side_effect = ClientError(
            {"Error": {"Code": "ParameterNotFound", "Message": "Parameter not found."}},
            operation_name='GetParameter'
        )

        # Llamada a la función
        parameter = AWSClients.get_parameter('test_parameter')

        # Afirmaciones
        self.assertEqual(parameter, "")
        mock_client.get_parameter.assert_called_once_with(Name='test_parameter', WithDecryption=False)




class TestGetSecret(unittest.TestCase):
    @patch("src.aws.clients.AWSClients.get_secrets_manager_client")
    @patch("src.logs.logger")
    def test_get_secret_success(self, mock_logger, mock_get_client):
        """
        Caso en el que el secreto se obtiene correctamente.
        """
        # Configurar el mock para que devuelva un secreto exitoso
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        secret_response = {
            "SecretString": json.dumps({"username": "admin", "password": "1234"})
        }
        mock_client.get_secret_value.return_value = secret_response

        secret_name = "my_secret"
        result = AWSClients.get_secret(secret_name)

        # Verificar que el resultado sea un diccionario con los datos correctos
        self.assertEqual(result, {"username": "admin", "password": "1234"})

        # Asegurarse de que no se registraron errores
        mock_logger.error.assert_not_called()

    @patch("src.aws.clients.AWSClients.get_secrets_manager_client")
    @patch("src.logs.logger")
    def test_get_secret_client_error(self, mock_logger, mock_get_client):
        """
        Caso en el que ocurre un error al obtener el secreto.
        """
        # Configurar el mock para que lance un ClientError
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_secret_value.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "GetSecretValue"
        )

        secret_name = "non_existent_secret"
        result = AWSClients.get_secret(secret_name)

        # Verificar que se devuelve un diccionario vacío en caso de error
        self.assertEqual(result, {})

