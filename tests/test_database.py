import os
import unittest
from unittest.mock import patch, MagicMock
from dotenv import load_dotenv

from src.connection.database import DataAccessLayer

# Carga las variables de entorno para la prueba
load_dotenv()


class TestDataAccessLayer(unittest.TestCase):
    @patch('src.aws.clients.AWSClients.get_secret')
    def test_connection_success(self, mock_get_secret):
        # Configura el secreto simulado
        mock_get_secret.return_value = {
            "USERNAME": os.getenv('USERNAME'),
            "PASSWORD": os.getenv('PASSWORD')
        }

        # Crea una instancia de DataAccessLayer
        dal = DataAccessLayer()

        # Verifica que la conexión y la sesión no sean None
        self.assertIsNotNone(dal.engine)
        self.assertIsNotNone(dal.session)

        # Cierra la sesión
        dal.close_session()

    @patch('src.aws.clients.AWSClients.get_secret')
    def test_connection_failure(self, mock_get_secret):
        # Simula que se produce un error al obtener secretos
        mock_get_secret.side_effect = Exception("Failed to get secrets")

        with self.assertRaises(Exception) as context:
            DataAccessLayer()

        self.assertIn("Failed to get secrets", str(context.exception))


if __name__ == '__main__':
    unittest.main()
