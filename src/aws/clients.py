import json
import os
import boto3
from botocore.exceptions import ClientError
from src.logs import logger as log


class AWSClients:
    _secrets_client = None
    _s3_client = None
    _sqs_client = None

    @classmethod
    def get_secrets_manager_client(cls):
        if cls._secrets_client is None:
            cls._secrets_client = cls._create_client('secretsmanager')
        return cls._secrets_client

    @classmethod
    def get_s3_client(cls):
        if cls._s3_client is None:
            cls._s3_client = cls._create_client('s3')
        return cls._s3_client

    @classmethod
    def get_sqs_client(cls):
        if cls._sqs_client is None:
            cls._sqs_client = cls._create_client('sqs')
        return cls._sqs_client

    @staticmethod
    def _create_client(service_name):
        # Si el entorno es 'local', conecta a LocalStack
        endpoint_url = None
        if os.getenv("APP_ENV") == "local":
            endpoint_url = "http://localhost:4566"
            log.logger.debug("Conectando a LocalStack para servicio: %s", service_name)

        return boto3.client(
            service_name,
            region_name="us-east-1",
            endpoint_url=endpoint_url
        )

    @staticmethod
    def get_secret(secret_name: str) -> dict:
        """
        Obtiene el secreto desde AWS Secrets Manager.
        """
        client = AWSClients.get_secrets_manager_client()
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            secret = get_secret_value_response["SecretString"]
            return json.loads(secret)
        except ClientError as e:
            log.logger.error("Error al obtener el secreto %s: %s", secret_name, e)
            return {}
