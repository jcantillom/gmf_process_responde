import unittest
from unittest.mock import MagicMock, patch
from sqlalchemy.orm import Session

from src.models.cgd_correo_parametro import CGDCorreosParametros
from src.services.error_handling_service import ErrorHandlingService
from src.config.config import env
from src.models.cgd_rta_pro_archivos import CGDRtaProArchivos


class TestErrorHandlingService(unittest.TestCase):
    @patch('src.services.aws_clients_service.AWSClients.get_ssm_client')
    def setUp(self, mock_get_ssm_client):
        # Mock de la base de datos

        mock_ssm_client = MagicMock()
        mock_get_ssm_client.return_value = mock_ssm_client

        mock_ssm_client.get_parameter.return_value = {
            'Parameter': {
                'Value': '{"special_start": "RE_ESP",'
                         ' "special_end": "0001", '
                         '"general_start": "RE_GEN", '
                         '"files-reponses-debito-reverso": "001,002", '
                         '"files-reponses-reintegros": "R", '
                         '"files-reponses-especiales": "ESP"}'
            }
        }
        self.mock_db = MagicMock(spec=Session)
        self.service = ErrorHandlingService(self.mock_db)
        # Mock de los repositorios y utilidades
        self.service.catalogo_error_repository.get_error_by_code = MagicMock()
        self.service.correo_parametro_repository.get_parameters_by_template = MagicMock()
        self.service.s3_utils.move_file_to_rechazados = MagicMock()
        self.service.archivo_validator.is_not_processed_state = MagicMock()
        self.service.rta_procesamiento_repository.update_state_rta_procesamiento = MagicMock()
        self.service.archivo_repository.update_estado_archivo = MagicMock()

    @patch("src.utils.sqs_utils.send_message_to_sqs")
    @patch("src.utils.sqs_utils.delete_message_from_sqs")
    @patch("src.utils.sqs_utils.build_email_message")
    @patch("src.utils.logger_utils")
    def test_handle_file_error(self, mock_logger, mock_build_email_message, mock_delete_message_from_sqs,
                               mock_send_message_to_sqs):
        # Datos de entrada para la función
        id_plantilla = "template123"
        filekey = "file-to-move.txt"
        bucket = "my-bucket"
        receipt_handle = "receipt123"
        codigo_error = "ERROR123"
        filename = "file1.txt"

        # Mock de respuestas
        self.service.catalogo_error_repository.get_error_by_code.return_value = MagicMock(
            codigo_error=codigo_error,
            descripcion="Error description"
        )

        # Aquí es donde ocurre el problema. Asegúrate de que estás devolviendo instancias correctas
        self.service.correo_parametro_repository.get_parameters_by_template.return_value = [
            CGDCorreosParametros(id_parametro="codigo_rechazo"),
            CGDCorreosParametros(id_parametro="descripcion_rechazo"),
            CGDCorreosParametros(id_parametro="fecha_recepcion"),
            CGDCorreosParametros(id_parametro="hora_recepcion"),
            CGDCorreosParametros(id_parametro="nombre_respuesta_pro_tu"),
            CGDCorreosParametros(id_parametro="plataforma_origen")
        ]

        self.service.archivo_validator.is_not_processed_state.return_value = True

        # Ejecutar la función que se está probando
        self.service.handle_error_master(id_plantilla, filekey, bucket, receipt_handle, codigo_error, filename)

        # Verificar que se movió el archivo a la carpeta de rechazados
        self.service.s3_utils.move_file_to_rechazados.assert_called_once_with(bucket, filekey)

        # Verificar que se obtuvo el error del catálogo
        self.service.catalogo_error_repository.get_error_by_code.assert_called_once_with(codigo_error)

    @patch("src.utils.logger_utils")
    def test_handle_unzip_error(self, mock_logger):
        # Datos de entrada para la función
        id_archivo = 123
        filekey = "file-to-move.txt"
        bucket_name = "my-bucket"
        receipt_handle = "receipt123"
        file_name = "file1.txt"
        contador_intentos_cargue = 2

        # Ejecutar la función que se está probando
        self.service.handle_generic_error(
            id_archivo=id_archivo,
            filekey=filekey,
            bucket_name=bucket_name,
            receipt_handle=receipt_handle,
            file_name=file_name,
            contador_intentos_cargue=contador_intentos_cargue,
            codigo_error=env.CONST_COD_ERROR_UNEXPECTED_FILE_COUNT,
            id_plantilla="template123"
        )

        # Verificar que se actualizó el estado de CGD_RTA_PROCESAMIENTO a "RECHAZADO"
        self.service.rta_procesamiento_repository.update_state_rta_procesamiento.assert_called_once_with(
            id_archivo=id_archivo,
            estado=env.CONST_ESTADO_REJECTED
        )

        # Verificar que se actualizó el estado del archivo a "PROCESAMIENTO_RECHAZADO"
        self.service.archivo_repository.update_estado_archivo.assert_called_once_with(
            file_name,
            env.CONST_ESTADO_PROCESAMIENTO_RECHAZADO,
            contador_intentos_cargue=contador_intentos_cargue
        )
