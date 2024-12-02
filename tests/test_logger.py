import unittest
import logging
from src.utils.logger_utils import CustomFormatter, get_logger


class TestLoggerCustomFormat(unittest.TestCase):

    def test_logger_custom_format_as_string(self):
        # Configurar logger y formateador
        formatter = CustomFormatter(use_json=False)

        # Crear un LogRecord simulado
        record = logging.LogRecord(
            name="test_logger",
            level=logging.DEBUG,
            pathname="test/path/to/file.py",
            lineno=42,
            msg="Mensaje de prueba",
            args=None,
            exc_info=None
        )

        # Agregar manualmente el atributo event_filename para simular el log
        record.event_filename = "test_file.txt"

        # Formatear el mensaje
        formatted_message = formatter.format(record)

        # Verificar el formato
        self.assertIn("[DEBUG]", formatted_message)
        self.assertIn("test/path/to/file.py:42", formatted_message)
        self.assertIn("[test_file.txt]", formatted_message)
        self.assertIn("- Mensaje de prueba", formatted_message)

    def test_logger_custom_format_without_file_name(self):
        # Configurar logger y formateador
        formatter = CustomFormatter(use_json=False)

        # Crear un LogRecord simulado
        record = logging.LogRecord(
            name="test_logger",
            level=logging.DEBUG,
            pathname="test/path/to/file.py",
            lineno=42,
            msg="Mensaje de prueba sin archivo",
            args=None,
            exc_info=None
        )

        # Formatear el mensaje
        formatted_message = formatter.format(record)

        # Verificar el formato sin file_name
        self.assertIn("[DEBUG]", formatted_message)
        self.assertIn("test/path/to/file.py:42", formatted_message)
        self.assertNotIn("[]", formatted_message)  # No debe incluir un file_name vacío
        self.assertIn("- Mensaje de prueba sin archivo", formatted_message)

    def test_logger_custom_format_as_json(self):
        # Configurar logger y formateador
        formatter = CustomFormatter(use_json=True)

        # Crear un LogRecord simulado
        record = logging.LogRecord(
            name="test_logger",
            level=logging.DEBUG,
            pathname="test/path/to/file.py",
            lineno=42,
            msg="Mensaje de prueba en JSON",
            args=None,
            exc_info=None
        )

        # Agregar manualmente el atributo event_filename para simular el log
        record.event_filename = "test_file.txt"

        # Formatear el mensaje
        formatted_message = formatter.format(record)

        # Verificar el formato JSON
        self.assertIn('"level": "DEBUG"', formatted_message)
        self.assertIn('"module_name": "test/path/to/file.py"', formatted_message)
        self.assertIn('"line_number": 42', formatted_message)
        self.assertIn('"file_name": "test_file.txt"', formatted_message)
        self.assertIn('"message": "Mensaje de prueba en JSON"', formatted_message)


class TestLogger(unittest.TestCase):

    def test_get_logger_debug_mode(self):
        logger = get_logger(debug_mode=True)
        self.assertEqual(logger.level, logging.DEBUG)

    def test_get_logger_info_mode(self):
        logger = get_logger(debug_mode=False)
        self.assertEqual(logger.level, logging.INFO)


