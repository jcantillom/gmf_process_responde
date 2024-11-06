# import unittest
# from unittest.mock import patch, MagicMock
# from sqlalchemy.orm import Session
# from src.services.archivo_service import ArchivoService
#
#
# class TestArchivoService(unittest.TestCase):
#
#     @patch('src.utils.event_utils.extract_filename_from_body', return_value="test_file.zip")
#     @patch('src.utils.event_utils.extract_bucket_from_body', return_value="01-bucketrtaprocesa-d01")
#     @patch('src.services.archivo_service.ArchivoService.validar_evento', return_value=True)
#     @patch('src.utils.sqs_utils.delete_message_from_sqs')
#     @patch('src.utils.s3_utils.S3Utils.check_file_exists_in_s3', return_value=True)
#     def test_validar_y_procesar_archivo_success(self, mock_check_file_exists_in_s3,
#                                                 mock_delete_message_from_sqs,
#                                                 mock_validar_evento,
#                                                 mock_extract_bucket_from_body,
#                                                 mock_extract_filename_from_body):
#         """
#         Test para validar y procesar archivo exitosamente.
#         """
#         event = {
#             "Records": [
#                 {
#                     "receiptHandle": "test_receipt_handle",
#                     "body": "{\"Records\":[{\"s3\":{\"object\":{\"key\":\"Recibidos/test_file.zip\"},\"bucket\":{\"name\":\"01-bucketrtaprocesa-d01\"}}}]}",
#                 }
#             ]
#         }
#
#         archivo_service = ArchivoService(MagicMock(spec=Session))
#         archivo_service.validar_y_procesar_archivo(event)
#
#         # Verificando que los mocks se llamaron como se esperaba
#         mock_extract_filename_from_body.assert_called_once()
#         mock_extract_bucket_from_body.assert_called_once()
#         mock_validar_evento.assert_called_once()
#         mock_check_file_exists_in_s3.assert_called_once_with("01-bucketrtaprocesa-d01", "Recibidos/test_file.zip")
#         mock_delete_message_from_sqs.assert_not_called()  # Si la eliminación no debe ocurrir en este caso
#
#
# if __name__ == '__main__':
#     unittest.main()
