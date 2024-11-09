import unittest
from unittest.mock import patch, MagicMock
from src.connection.database import DataAccessLayer


class TestDataAccessLayer(unittest.TestCase):
    @patch.dict('os.environ', {
        'USERNAME': 'test_user',
        'PASSWORD': 'test_password',
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_NAME': 'test_db',
        'SECRETS_DB': 'test_secret'
    })
    @patch('src.aws.clients.AWSClients.get_secret')
    @patch('src.connection.database.create_engine')
    @patch('src.connection.database.sessionmaker')
    def test_connection_success(self, mock_sessionmaker, mock_create_engine, mock_get_secret):
        # Configura el secreto simulado
        mock_get_secret.return_value = {
            "USERNAME": 'test_user',
            "PASSWORD": 'test_password'
        }

        # Mock de la conexión de la base de datos
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Mock de la sesión
        mock_session = MagicMock()
        mock_sessionmaker.return_value = mock_session

        # Crea una instancia de DataAccessLayer
        dal = DataAccessLayer()

        # Verifica que la conexión y la sesión no sean None
        self.assertIsNotNone(dal.engine)
        self.assertIsNotNone(dal.session)

        # Cierra la sesión
        dal.close_session()

    @patch.dict('os.environ', {
        'USERNAME': 'test_user',
        'PASSWORD': 'test_password',
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_NAME': 'test_db',
        'SECRETS_DB': 'test_secret'
    })
    @patch('src.aws.clients.AWSClients.get_secret')
    def test_connection_failure(self, mock_get_secret):
        # Simula que se produce un error al obtener secretos
        mock_get_secret.side_effect = Exception("Failed to get secrets")

        with self.assertRaises(Exception) as context:
            DataAccessLayer()

        self.assertIn("Failed to get secrets", str(context.exception))

    @patch('src.connection.database.DataAccessLayer')
    def test_session_scope_commit(self, mock_dal_instance):
        # Configura el mock de DataAccessLayer
        self.mock_session = MagicMock()
        self.mock_dal = MagicMock()
        self.mock_dal.session = self.mock_session
        self.mock_dal_instance = MagicMock(return_value=self.mock_dal)

        # Crea una instancia de DataAccessLayer
        dal = self.mock_dal_instance()

        # Ejecuta el método session_scope
        with dal.session_scope() as session:
            # Verifica que se haya llamado el método commit
            yield self.mock_session
            self.mock_session.commit.assert_called_once()
