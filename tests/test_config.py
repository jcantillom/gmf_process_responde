import unittest
from unittest.mock import patch, MagicMock
from pydantic import ValidationError
from src.config.config import EnvironmentSettings


class TestEnvironmentSettings(unittest.TestCase):

    @patch.dict('os.environ', {
        'APP_ENV': 'local',
        'SECRETS_DB': 'NGMF_RDS_POSTGRES_CREDENTIALS',
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',  # Debe ser un string para que Pydantic lo convierta correctamente
        'DB_NAME': 'postgres',
        'DEBUG_MODE': 'True',
        'SQS_URL_PRO_RESPONSE_TO_PROCESS': 'http://sqs.url',
        'SQS_URL_EMAILS': 'http://sqs.url',
        'SQS_URL_PRO_RESPONSE_TO_UPLOAD': 'http://sqs.url',
        'SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE': 'http://sqs.url',
        'PARAMETER_STORE_FILE_CONFIG': '/path/to/config',
        'SPECIAL_START_NAME': 'start',
        'SPECIAL_END_NAME': 'end',
        'GENERAL_START_NAME': 'general',
        'VALID_STATES_FILES': 'valid_states',
        'SUFFIX_RESPONSE_DEBITO': 'debit',
        'SUFFIX_RESPONSE_REINTEGROS': 'reinstate',
        'SUFFIX_RESPONSE_ESPECIALES': 'specials',
        'CONST_PRE_SPECIAL_FILE': 'RE_ESP',
        'CONST_PRE_GENERAL_FILE': 'RE_PRO',
        'CONST_ID_PLANTILLA_EMAIL': 'PC009',
        'CONST_COD_ERROR_EMAIL': 'EPCM001',
        'CONST_ESTADO_PROCESSED': 'PROCESADO',
        'CONST_ESTADO_LOAD_RTA_PROCESSING': 'CARGANDO_RTA_PROCESAMIENTO',
        'CONST_ESTADO_INICIADO': 'INICIADO',
        'CONST_ESTADO_SEND': 'ENVIADO',
        'CONST_ESTADO_REJECTED': 'RECHAZADO',
        'CONST_ESTADO_INIT_PENDING': 'PENDIENTE_INICIO',
        'CONST_ESTADO_PROCESAMIENTO_RECHAZADO': 'PROCESAMIENTO_RECHAZADO',
        'CONST_ID_PLANTILLA_CORREO_ERROR_DECOMPRESION': 'PC009',
        'CONST_COD_ERROR_DECOMPRESION': 'EPRO004',
        'S3_BUCKET_NAME': 'my-bucket',
        'DIR_RECEPTION_FILES': 'Recibidos',
        'DIR_PROCESSED_FILES': 'Procesados',
        'DIR_REJECTED_FILES': 'Rechazados',
        'DIR_PROCESSING_FILES': 'Procesando',
    })
    def test_environment_settings_success(self):
        """Prueba que las variables de entorno se cargan correctamente."""
        env = EnvironmentSettings()
        self.assertEqual(env.APP_ENV, 'local')
        self.assertEqual(env.DB_HOST, 'localhost')
        self.assertEqual(env.DEBUG_MODE, True)
        self.assertEqual(env.DB_PORT, 5432)

    @patch.dict('os.environ', {
        'APP_ENV': 'local',
        'SECRETS_DB': 'NGMF_RDS_POSTGRES_CREDENTIALS',
        'DB_HOST': 'localhost',
        'DB_PORT': 'not_an_integer',  # Valor inválido para provocar un error
        'DB_NAME': 'postgres',
        'DEBUG_MODE': 'True',
        'SQS_URL_PRO_RESPONSE_TO_PROCESS': 'http://sqs.url',
    })
    def test_environment_settings_failure(self):
        """Prueba que se lanza una excepción al cargar variables de entorno inválidas."""
        with self.assertRaises(ValidationError):
            EnvironmentSettings()


if __name__ == '__main__':
    unittest.main()
