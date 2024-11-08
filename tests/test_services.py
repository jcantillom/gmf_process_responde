import unittest
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session
from src.services.cgd_rta_pro_archivo_service import CGDRtaProArchivosService
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.config.config import env
from src.utils.sqs_utils import send_message_to_sqs


class TestCGDRtaProArchivosService(unittest.TestCase):
    def setUp(self):
        self.mock_db = MagicMock(spec=Session)
        self.service = CGDRtaProArchivosService(self.mock_db)
        self.service.cgd_rta_pro_archivos_repository = MagicMock()

    @patch("src.logs.logger")
    def test_register_extracted_files(self, mock_logger):
        """
        Caso en el que los archivos descomprimidos se registran correctamente en la base de datos.
        """
        # Datos de entrada
        id_archivo = 123
        id_rta_procesamiento = 456
        extracted_files = ["folder/archivo-01.txt", "folder/archivo-02.txt"]

        # Llamar a la función
        self.service.register_extracted_files(id_archivo, id_rta_procesamiento, extracted_files)

        # Verificar que se insertaron correctamente las entradas en la base de datos
        self.assertEqual(self.service.cgd_rta_pro_archivos_repository.insert.call_count, 2)

        # Verificar que se registró un mensaje de INFO
        mock_logger.info.assert_called_once_with("Archivos descomprimidos registrados en CGD_RTA_PRO_ARCHIVOS")

    @patch("src.utils.sqs_utils.send_message_to_sqs")
    @patch("src.logs.logger")
    def test_send_pending_files_to_queue_by_id(self, mock_logger, mock_send_message):
        """
        Caso en el que los archivos pendientes se envían correctamente a la cola SQS.
        """
        # Datos de entrada
        id_archivo = 123
        queue_url = "https://sqs.test-queue.amazonaws.com/123456789/test-queue"
        destination_folder = "processed"

        # Simular archivos pendientes en el repositorio
        pending_files = [
            MagicMock(id_archivo=123, id_rta_procesamiento=456, nombre_archivo="archivo-01.txt"),
            MagicMock(id_archivo=123, id_rta_procesamiento=456, nombre_archivo="archivo-02.txt"),
        ]
        self.service.cgd_rta_pro_archivos_repository.get_pending_files_by_id_archivo.return_value = pending_files

        # Llamar a la función
        self.service.send_pending_files_to_queue_by_id(id_archivo, queue_url, destination_folder)

        mock_send_message.assert_any_call(
            queue_url,
            {
                "bucket_name": env.S3_BUCKET_NAME,
                "folder_name": destination_folder,
                "file_name": "archivo-01.txt",
                "file_id": 123,
                "response_processing_id": 456,
            },
            "archivo-01.txt"
        )
        mock_send_message.assert_any_call(
            queue_url,
            {
                "bucket_name": env.S3_BUCKET_NAME,
                "folder_name": destination_folder,
                "file_name": "archivo-02.txt",
                "file_id": 123,
                "response_processing_id": 456,
            },
            "archivo-02.txt"
        )

        # Verificar que se actualizaron los estados en la base de datos
        self.assertEqual(self.service.cgd_rta_pro_archivos_repository.update_estado_to_enviado.call_count, 2)

        # Verificar que se registró un mensaje de DEBUG
        mock_logger.debug.assert_called_once_with("Estado actualizado a 'ENVIADO' para archivo con ID 123")


if __name__ == "__main__":
    unittest.main()
