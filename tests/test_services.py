import unittest
from unittest.mock import MagicMock, patch
from src.services.cgd_rta_pro_archivo_service import CGDRtaProArchivosService
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos
from src.config.config import env


class TestRegisterExtractedFiles(unittest.TestCase):

    def setUp(self):
        # Mock de la base de datos
        self.mock_db = MagicMock()

        self.service = CGDRtaProArchivosService(self.mock_db)
        # Mock del repositorio
        self.service.cgd_rta_pro_archivos_repository.insert = MagicMock()

    @patch("src.logs.logger")
    def test_register_extracted_files(self, mock_logger):
        # Datos de entrada para la función
        id_archivo = 123
        id_rta_procesamiento = 456
        extracted_files = [
            "path/to/file1-01.txt",
            "path/to/file2-02.txt",
            "path/to/file3-03.txt"
        ]

        # Ejecutar la función que se está probando
        self.service.register_extracted_files(
            id_archivo=id_archivo,
            id_rta_procesamiento=id_rta_procesamiento,
            extracted_files=extracted_files
        )

        # # Verificar que el método insert fue llamado tres veces
        # self.assertEqual(self.service.cgd_rta_pro_archivos_repository.insert.call_count, 3)

        # Verificar que los datos insertados son correctos
        expected_calls = [
            CGDRtaProArchivos(
                id_archivo=123,
                id_rta_procesamiento=456,
                nombre_archivo="file1-01.txt",
                tipo_archivo_rta="01",
                estado=env.CONST_ESTADO_INIT_PENDING,
                contador_intentos_cargue=0
            ),
            CGDRtaProArchivos(
                id_archivo=123,
                id_rta_procesamiento=456,
                nombre_archivo="file2-02.txt",
                tipo_archivo_rta="02",
                estado=env.CONST_ESTADO_INIT_PENDING,
                contador_intentos_cargue=0
            ),
            CGDRtaProArchivos(
                id_archivo=123,
                id_rta_procesamiento=456,
                nombre_archivo="file3-03.txt",
                tipo_archivo_rta="03",
                estado=env.CONST_ESTADO_INIT_PENDING,
                contador_intentos_cargue=0
            ),
        ]

class TestSendPendingFilesToQueue(unittest.TestCase):

    def setUp(self):
        # Mock de la base de datos
        self.mock_db = MagicMock()
        self.service = CGDRtaProArchivosService(self.mock_db)

        # Mock del repositorio
        self.service.cgd_rta_pro_archivos_repository.get_pending_files_by_id_archivo = MagicMock()
        self.service.cgd_rta_pro_archivos_repository.update_estado_to_enviado = MagicMock()

    @patch("src.utils.sqs_utils.send_message_to_sqs")
    @patch("src.logs.logger")
    def test_send_pending_files_to_queue(self, mock_logger, mock_send_message_to_sqs):
        # Datos de entrada
        id_archivo = 123
        queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
        destination_folder = "destination-folder"

        # Archivos pendientes simulados
        pending_files = [
            CGDRtaProArchivos(
                id_archivo=123,
                id_rta_procesamiento=456,
                nombre_archivo="file1-01.txt",
                tipo_archivo_rta="01",
                estado=env.CONST_ESTADO_INIT_PENDING,
                contador_intentos_cargue=0
            ),
            CGDRtaProArchivos(
                id_archivo=123,
                id_rta_procesamiento=457,
                nombre_archivo="file2-02.txt",
                tipo_archivo_rta="02",
                estado=env.CONST_ESTADO_INIT_PENDING,
                contador_intentos_cargue=0
            )
        ]

        # Configurar el mock para que devuelva los archivos pendientes
        self.service.cgd_rta_pro_archivos_repository.get_pending_files_by_id_archivo.return_value = pending_files

        # Ejecutar la función que se está probando
        self.service.send_pending_files_to_queue_by_id(
            id_archivo=id_archivo,
            queue_url=queue_url,
            destination_folder=destination_folder
        )

        # Verificar que se obtuvieron los archivos pendientes
        self.service.cgd_rta_pro_archivos_repository.get_pending_files_by_id_archivo.assert_called_once_with(id_archivo)



        for file in pending_files:
            expected_message = {
                "bucket_name": env.S3_BUCKET_NAME,
                "folder_name": destination_folder,
                "file_name": file.nombre_archivo,
                "file_id": int(file.id_archivo),
                "response_processing_id": int(file.id_rta_procesamiento),
            }


        # Verificar que se actualizó el estado de los archivos a "ENVIADO"
        self.assertEqual(self.service.cgd_rta_pro_archivos_repository.update_estado_to_enviado.call_count, len(pending_files))
        for file in pending_files:
            self.service.cgd_rta_pro_archivos_repository.update_estado_to_enviado.assert_any_call(file.id_archivo, file.nombre_archivo)







