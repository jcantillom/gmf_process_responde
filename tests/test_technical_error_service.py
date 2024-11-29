import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.services.technical_error_service import TechnicalErrorService
from src.config.config import env
import json


class TestTechnicalErrorService(unittest.TestCase):
    @patch("src.services.aws_clients_service.AWSClients.get_ssm_client")
    def setUp(self, mock_ssm_client):
        # Mockear la sesión de base de datos
        mock_ssm_instance = mock_ssm_client.return_value
        mock_ssm_instance.get_parameter.return_value = {
            'Parameter': {
                'Value': '{"CONFIG_NAME": "some_value"}'
            }
        }
        self.mock_db = MagicMock()

        # Instanciar la clase con los mocks
        self.service = TechnicalErrorService(db=self.mock_db)

        # Mockear repositorios
        self.service.archivo_repository = MagicMock()
        self.service.rta_procesamiento_repository = MagicMock()
        self.service.archivo_estado_repository = MagicMock()
        self.service.error_handling_service = MagicMock()

        # Configurar valores de prueba
        self.event = {
            "Records": [
                {
                    "messageId": "32a6dcc3-992e-4622-8437-1ff1c11f883c",
                    "receiptHandle": "YjA0YjNjNWEtNWM5ZS00YzI0LTk0MDgtMjUyNmZhNGZhZDIzIGFybjphd3M6c3FzOnVzLWVhc3QtMTowMDAwMDAwMDAwMDA6ZW1haWxzLXRvLXNlbmQgZTk0MWQwMjktOWQ3Ny00MGMyLWJmZTgtMmEwZGJkMWJlYTFiIDE3MzEyMTc0MTYuMTM3MTAyNg==",
                    "body": "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2020-09-25T15:43:27.121Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:EXAMPLE\"},\"requestParameters\":{\"sourceIPAddress\":\"205.255.255.255\"},\"responseElements\":{\"x-amz-request-id\":\"EXAMPLE123456789\",\"x-amz-id-2\":\"EXAMPLE123/5678ABCDEFGHIJK12345EXAMPLE=\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"testConfigRule\",\"bucket\":{\"name\":\"01-bucketrtaprocesa-d01\",\"ownerIdentity\":{\"principalId\":\"EXAMPLE\"},\"arn\":\"arn:aws:s3:::example-bucket\"},\"object\":{\"key\":\"Recibidos/RE_PRO_TUTGMF0001003920241021-5001.zip\",\"size\":1024,\"eTag\":\"0123456789abcdef0123456789abcdef\",\"sequencer\":\"0A1B2C3D4E5F678901\"}}}]}",
                    "attributes": {
                        "ApproximateReceiveCount": "2",
                        "SentTimestamp": "1721084544748",
                        "SenderId": "000000000000",
                        "ApproximateFirstReceiveTimestamp": "1721084544820"
                    },
                    "messageAttributes": {},
                    "md5OfBody": "2fd1355d7becb7a1460d1b5fc54c0095",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:my-queue",
                    "awsRegion": "us-east-1"
                }
            ]
        }

        self.acg_nombre_archivo = "RE_PRO_TestFile_001"
        self.file_id_and_response_processing_id_in_event = True
        self.max_retries = 3
        self.retry_delay = 900
        self.estado_inicial = "EN_PROCESO"
        self.receipt_handle = "test-receipt-handle"
        self.code_error = "ERROR_TEST"
        self.detail_error = "Detalle del error"
        self.id_archivo = 1
        self.id_rta_procesamiento = 1

    def test_handle_technical_error_inserts_in_rta_procesamiento(self):
        # Configurar mocks
        self.service.archivo_repository.get_archivo_by_nombre_archivo.return_value.id_archivo = 1
        self.service.rta_procesamiento_repository.insert_code_error = MagicMock()

        # Ejecutar la función
        self.service.handle_technical_error(
            event=self.event,
            code_error=self.code_error,
            detail_error=self.detail_error,
            acg_nombre_archivo=self.acg_nombre_archivo,
            file_id_and_response_processing_id_in_event=self.file_id_and_response_processing_id_in_event,
            max_retries=self.max_retries,
            retry_delay=self.retry_delay,
            estado_inicial=self.estado_inicial,
            receipt_handle=self.receipt_handle,
            file_name="test-file.zip"
        )

        # Verificar que se llamó a insert_code_error
        self.service.rta_procesamiento_repository.insert_code_error.assert_called_once()
