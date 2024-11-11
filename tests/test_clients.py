import json
import os
import unittest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from src.aws.clients import AWSClients


class TestAWSClients(unittest.TestCase):

    @patch('boto3.client')
    @patch.dict('os.environ', {'APP_ENV': 'local', 'AWS_REGION': 'us-east-1'})
    def test_get_secrets_manager_client(self, mock_boto_client):
        # Crear un mock para el cliente de Secrets Manager
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Llamar al método que estás probando
        secrets_manager_client = AWSClients.get_secrets_manager_client()

        # Verificar que se llamó con los argumentos esperados
        mock_boto_client.assert_called_once_with(
            'secretsmanager',
            region_name='us-east-1',
            endpoint_url='http://localhost:4566'
        )
        self.assertEqual(secrets_manager_client, mock_client)

    @patch('boto3.client')
    @patch.dict('os.environ', {'APP_ENV': 'local', 'AWS_REGION': 'us-east-1'})
    def test_get_s3_client(self, mock_boto_client):
        # Crear un mock para el cliente de S3
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Llamar al método que estás probando
        s3_client = AWSClients.get_s3_client()

        # Verificar que se llamó con los argumentos esperados
        # mock_boto_client.assert_called_once_with(
        #     's3',
        #     region_name='us-east-1',
        #     endpoint_url='http://localhost:4566'
        # )
        # self.assertEqual(s3_client, mock_client)

    @patch('boto3.client')
    @patch.dict('os.environ', {'APP_ENV': 'local', 'AWS_REGION': 'us-east-1'})
    def test_get_sqs_client(self, mock_boto_client):
        # Crear un mock para el cliente de SQS
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Llamar al método que estás probando
        sqs_client = AWSClients.get_sqs_client()

        # # Verificar que se llamó con los argumentos esperados
        # mock_boto_client.assert_called_once_with(
        #     'sqs',
        #     region_name='us-east-1',
        #     endpoint_url='http://localhost:4566'
        # )
        # self.assertEqual(sqs_client, mock_client)

    @patch('boto3.client')
    @patch.dict('os.environ', {'APP_ENV': 'local', 'AWS_REGION': 'us-east-1'})
    def test_get_ssm_client(self, mock_boto_client):
        # Crear un mock para el cliente de SSM
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        # Llamar al método que estás probando
        ssm_client = AWSClients.get_ssm_client()

        # Verificar que se llamó con los argumentos esperados
        mock_boto_client.assert_called_once_with(
            'ssm',
            region_name='us-east-1',
            endpoint_url='http://localhost:4566'
        )
        self.assertEqual(ssm_client, mock_client)

    @patch("src.aws.clients.AWSClients.get_secrets_manager_client")
    @patch("src.logs.logger")
    def test_get_secret_success(self, mock_logger, mock_get_client):
        # Configurar el cliente simulado
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        secret_response = {
            "SecretString": json.dumps({"username": "admin", "password": "1234"})
        }
        mock_client.get_secret_value.return_value = secret_response

        secret = AWSClients.get_secret("my_secret")
        self.assertEqual(secret, {"username": "admin", "password": "1234"})
        mock_logger.error.assert_not_called()

    @patch('src.aws.clients.AWSClients.get_secrets_manager_client')
    def test_get_secret_client_error(self, mock_get_client):
        # Mock para simular un error al obtener el secreto
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.get_secret_value.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "GetSecretValue"
        )

        secret = AWSClients.get_secret('test_secret')
        self.assertEqual(secret, {})
        mock_client.get_secret_value.assert_called_once_with(SecretId='test_secret')
