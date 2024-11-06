import unittest
import logging
from unittest.mock import patch

from src.logs.logger import CustomFormatter, get_logger


class TestLoggerCustomFormat(unittest.TestCase):

    def test_logger_custom_format(self):
        logger = get_logger(debug_mode=True)

        # Crea un handler y un formatter reales
        stream_handler = logging.StreamHandler()
        formatter = CustomFormatter()  # Instancia real del formateador
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # Crea un LogRecord simulado
        record = logging.LogRecord(
            name=logger.name,
            level=logging.DEBUG,
            pathname='test/path/to/file.py',
            lineno=42,
            msg='Mensaje de prueba',
            args=None,
            exc_info=None
        )

        # Formatear el mensaje usando el formateador real
        formatted_message = logger.handlers[0].formatter.format(record)

        # Verifica que el mensaje formateado contiene la información esperada
        self.assertIn("[DEBUG]", formatted_message)
        self.assertIn("[test/path/to/file.py:42]", formatted_message)
        self.assertIn("- Mensaje de prueba", formatted_message)


class TestLogger(unittest.TestCase):

    def test_get_logger_debug_mode(self):
        logger = get_logger(debug_mode=True)
        self.assertEqual(logger.level, logging.DEBUG)

    def test_get_logger_info_mode(self):
        logger = get_logger(debug_mode=False)
        self.assertEqual(logger.level, logging.INFO)

    def test_logger_formatting(self):
        logger = get_logger(debug_mode=True)

        # Use the actual StreamHandler and CustomFormatter
        stream_handler = logging.StreamHandler()
        formatter = CustomFormatter()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # Simula un mensaje de log
        logger.debug("Este es un mensaje de debug")

        # Create a LogRecord for checking formatting
        record = logging.LogRecord(
            name=logger.name,
            level=logging.DEBUG,
            pathname='test/path/to/file.py',
            lineno=55,
            msg='Este es un mensaje de debug',
            args=None,
            exc_info=None
        )

        # Format the message using the real formatter
        formatted_message = logger.handlers[0].formatter.format(record)

        # Check that the formatted message includes the expected components
        self.assertIn("[DEBUG]", formatted_message)
        self.assertIn("[test/path/to/file.py:55]", formatted_message)
        self.assertIn("- Este es un mensaje de debug", formatted_message)

    def test_logger_custom_format(self):
        logger = get_logger(debug_mode=True)

        # Create a handler and use the real formatter
        stream_handler = logging.StreamHandler()
        formatter = CustomFormatter()  # Use the real formatter
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        # Create a LogRecord simulated
        record = logging.LogRecord(
            name=logger.name,
            level=logging.DEBUG,
            pathname='test/path/to/file.py',
            lineno=42,
            msg='Mensaje de prueba',
            args=None,
            exc_info=None
        )

        # Format the message using the real formatter
        formatted_message = logger.handlers[0].formatter.format(record)

        # Verify that the formatted message contains the expected information
        self.assertIn("[DEBUG]", formatted_message)
        self.assertIn("[test/path/to/file.py:42]", formatted_message)
        self.assertIn("- Mensaje de prueba", formatted_message)


if __name__ == '__main__':
    unittest.main()
