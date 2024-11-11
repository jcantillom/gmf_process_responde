import unittest
from unittest.mock import MagicMock
from src.controllers.archivo_controller import process_sqs_message
from src.services.archivo_service import ArchivoService
from sqlalchemy.orm import Session


class TestArchivoController(unittest.TestCase):

    def setUp(self):
        # Configura el evento simulado que se pasará al controlador
        self.event = {
            "Records": [
                {
                    "messageId": "32a6dcc3-992e-4622-8437-1ff1c11f883c",
                    "body": "{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",\"bucket\":{\"name\":\"example-bucket\"},\"object\":{\"key\":\"example-key.zip\"}}]}",
                }
            ]
        }
        # Crea un mock para la sesión de la base de datos
        self.db_mock = MagicMock(spec=Session)
        # Crea un mock para el servicio de ArchivoService
        self.archivo_service_mock = MagicMock(spec=ArchivoService)

    def test_process_sqs_message_success(self):
        # Parchea el servicio para que use el mock en lugar de la implementación real
        with unittest.mock.patch('src.controllers.archivo_controller.ArchivoService',
                                 return_value=self.archivo_service_mock):
            process_sqs_message(self.event, self.db_mock)

            self.archivo_service_mock.validar_y_procesar_archivo.assert_called_once_with(self.event)

    def test_process_sqs_message_error(self):
        self.archivo_service_mock.validar_y_procesar_archivo.side_effect = Exception(
            "Error en el procesamiento del archivo")

        # Parchea el servicio para que use el mock en lugar de la implementación real
        with unittest.mock.patch('src.controllers.archivo_controller.ArchivoService',
                                 return_value=self.archivo_service_mock):
            with self.assertRaises(Exception) as context:
                process_sqs_message(self.event, self.db_mock)

            self.assertIn("Error en el procesamiento del archivo", str(context.exception))
