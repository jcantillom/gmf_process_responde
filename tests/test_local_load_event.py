import unittest
from unittest.mock import MagicMock, patch, mock_open
import json
from local.load_event import load_local_event


class TestLoadLocalEvent(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data='{"key1": "value1"}')
    def test_load_local_event(self, mock_file):
        """
        Prueba cargar un archivo JSON valido
        """
        result = load_local_event()
        self.assertEqual(result, {'key1': 'value1'})

    @patch("builtins.open", new_callable=mock_open, read_data='{key: "value"}')
    def test_json_decode_error(self, mock_file):
        """Prueba un error de formato en el archivo JSON"""
        result = load_local_event()
        self.assertEqual(result, {})

    # Prueba de que el archivo no existe
    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_file_not_found(self, mock_file):
        """Prueba que el archivo no existe"""
        result = load_local_event()
        self.assertEqual(result, {})
