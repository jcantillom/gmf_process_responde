import unittest
from unittest.mock import patch, MagicMock
from src.utils.validator_utils import ArchivoValidator  # Ajusta la ruta según tu estructura
import json
import os


class TestArchivoValidator(unittest.TestCase):

    @patch('src.aws.clients.AWSClients.get_ssm_client')  # Asegúrate de que esta ruta sea correcta
    def test_get_file_config_name_success(self, mock_get_ssm_client):
        # Configuración del mock para el cliente SSM
        parameter_name = '/gmf/process-responses/general-config'
        mock_response = {
            'Parameter': {
                'Value': json.dumps({
                    'special_start': 'start_value',
                    'special_end': 'end_value',
                    'general_start': 'general_value',
                    'files-reponses-debito-reverso': 'file1,file2',
                    'files-reponses-reintegros': 'file3,file4',
                    'files-reponses-especiales': 'file5,file6'
                })
            }
        }
        # Crear un cliente simulado para SSM
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = mock_response
        mock_get_ssm_client.return_value = mock_ssm  # Aquí estamos configurando el retorno del cliente

        # Llama a la función sin pasar el cliente directamente
        validator = ArchivoValidator()

        # Verificar que se llamó a get_parameter con el nombre correcto
        mock_ssm.get_parameter.assert_called_once_with(Name=parameter_name)

    # test is_special_prefix
    def test_is_special_prefix(self):
        # Arrange
        filename = 'RE_ESP_file1'
        # Act
        result = ArchivoValidator.is_special_prefix(filename)
        # Assert
        self.assertTrue(result)

    # test is_valid_date_in_filename
    def test_is_valid_date_in_filename(self):
        # Arrange
        fecha_str = '20221231'
        # Act
        result = ArchivoValidator.is_valid_date_in_filename(fecha_str)
        # Assert
        self.assertTrue(result)

    @patch('src.aws.clients.AWSClients.get_ssm_client')  # Parchea la función que obtiene el cliente SSM
    @patch.dict(os.environ, {
        'PARAMETER_STORE_FILE_CONFIG': '/gmf/process-responses/general-config',
        'SPECIAL_START_NAME': 'special_start',
        'SPECIAL_END_NAME': 'special_end'
    })
    def test_is_special_file(self, mock_get_ssm_client):  # Aquí añadimos el argumento
        # Simulación de la respuesta del SSM
        mock_response = {
            'Parameter': {
                'Value': json.dumps({
                    'special_start': 'RE_ESP_TUTGMF',
                    'special_end': '0001',
                })
            }
        }

        # Crear un cliente simulado para SSM
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = mock_response
        mock_get_ssm_client.return_value = mock_ssm  # Configuramos el retorno del cliente

        # Llama a la función
        validator = ArchivoValidator()  # Se espera que este constructor llame internamente a get_ssm_client

        # Arrange
        filename = 'RE_ESP_TUTGMF0001003920241002-0001'  # Cambia esto a un nombre que esperas que pase

        # Act
        result = validator.is_special_file(filename)

        # Assert
        self.assertTrue(result)

    # Debe ser verdadero si cumple el patrón


if __name__ == '__main__':
    unittest.main()
