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

    @patch('boto3.client')
    @patch.dict('os.environ', {'APP_ENV': 'local'})
    def test_get_ssm_client(self, mock_boto_client):
        client = AWSClients.get_ssm_client()
        mock_boto_client.assert_called_once_with('ssm', region_name='us-east-1', endpoint_url='http://localhost:4566')
        self.assertEqual(client, mock_boto_client.return_value)

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


if __name__ == '__main__':
    unittest.main()
