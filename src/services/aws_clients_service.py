import json
import os
import boto3
from botocore.exceptions import ClientError
from src.utils.logger_utils import get_logger
from src.utils.singleton import SingletonMeta
from src.config.config import env

logger = get_logger(env.DEBUG_MODE)


class AWSClients(metaclass=SingletonMeta):
    _ssm_client = None
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

    @classmethod
    def get_ssm_client(cls):
        if cls._ssm_client is None:
            cls._ssm_client = cls._create_client('ssm')
        return cls._ssm_client

    @staticmethod
    def _create_client(service_name):
        endpoint_url = None
        if os.getenv("APP_ENV") == "local":
            endpoint_url = "http://localhost:4566"
            logger.debug("Conectando a LocalStack para servicio: %s", service_name)

        return boto3.client(
            service_name,
            region_name="us-east-1",
            endpoint_url=endpoint_url
        )

    @staticmethod
    def get_secret(secret_name: str) -> dict:
        client = AWSClients.get_secrets_manager_client()
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            secret = get_secret_value_response["SecretString"]
            return json.loads(secret)
        except ClientError as e:
            logger.error("Error al obtener el secreto %s: %s", secret_name, e)
            return {}

    @staticmethod
    def get_parameter(parameter_name: str) -> str:
        client = AWSClients.get_ssm_client()

        try:
            parameter_response = client.get_parameter(Name=parameter_name, WithDecryption=False)
            parameter_value = parameter_response["Parameter"]["Value"]
            logger.debug("Parámetro obtenido correctamente: %s", parameter_name)
            return parameter_value
        except ClientError as e:
            logger.error("Error al obtener el parámetro %s: %s", parameter_name, e)
            return ""
