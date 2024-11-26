import unittest
from unittest.mock import patch, MagicMock
from src.config.lambda_init import initialize_lambda
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module='sqlalchemy')


class TestLambdaInit(unittest.TestCase):

    @patch('src.config.lambda_init.env')
    @patch('src.config.lambda_init.DataAccessLayer')
    @patch('src.config.lambda_init.process_sqs_message')
    @patch('src.config.lambda_init.get_logger')
    def test_initialize_lambda_local_env(self,
                                         mock_get_logger,
                                         mock_process_sqs_message,
                                         mock_dataclasses,
                                         mock_env):
        # Configurar los mocks
        mock_env.APP_ENV = "dev"
        mock_env.DEBUG_MODE = True
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_dal_instance = MagicMock()
        mock_dataclasses.return_value = mock_dal_instance
        mock_session = MagicMock()
        mock_dal_instance.session_scope.return_value.__enter__.return_value = mock_session

        # Ejecutar la función
        event = {}
        context = {}
        initialize_lambda(event, context)

        # Verificar que se llamaron los métodos esperados
        mock_dataclasses.assert_called_once()
        mock_dal_instance.session_scope.assert_called_once()
        mock_logger.info.assert_called_once_with("Proceso de Lambda completado")

    @patch('src.config.lambda_init.env')
    @patch('src.config.lambda_init.DataAccessLayer')
    @patch('src.config.lambda_init.process_sqs_message')
    @patch('src.config.lambda_init.get_logger')
    def test_initialize_lambda_non_local_env(self, mock_get_logger, mock_process_sqs_message, mock_DataAccessLayer,
                                             mock_env):
        # Configurar los mocks
        mock_env.APP_ENV = "production"
        mock_env.DEBUG_MODE = False
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_dal_instance = MagicMock()
        mock_DataAccessLayer.return_value = mock_dal_instance
        mock_session = MagicMock()
        mock_dal_instance.session_scope.return_value.__enter__.return_value = mock_session

        # Ejecutar la función
        event = {'key': 'value'}
        context = {}
        initialize_lambda(event, context)


        mock_DataAccessLayer.assert_called_once()
        mock_dal_instance.session_scope.assert_called_once()
        mock_process_sqs_message.assert_called_once_with(event, mock_session)
        mock_logger.info.assert_called_once_with("Proceso de Lambda completado")

    @patch('src.config.lambda_init.env')
    @patch('src.config.lambda_init.DataAccessLayer')
    @patch('src.config.lambda_init.process_sqs_message')
    @patch('src.config.lambda_init.get_logger')
    def test_initialize_lambda_exception_handling(self, mock_get_logger, mock_process_sqs_message,
                                                  mock_DataAccessLayer, mock_env):
        # Configurar los mocks
        mock_env.APP_ENV = "dev"
        mock_env.DEBUG_MODE = True
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_dal_instance = MagicMock()
        mock_DataAccessLayer.return_value = mock_dal_instance
        mock_session = MagicMock()
        mock_dal_instance.session_scope.return_value.__enter__.return_value = mock_session

        # Simular una excepción al procesar el mensaje
        mock_process_sqs_message.side_effect = Exception("Error processing message")

        # Ejecutar la función y verificar que se maneje la excepción
        event = {}
        context = {}
        with self.assertRaises(Exception):
            initialize_lambda(event, context)

        mock_logger.error.assert_called_once_with("Error al inicializar la Lambda: Error processing message")
