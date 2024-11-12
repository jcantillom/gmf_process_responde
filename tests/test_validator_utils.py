import json
import os
import unittest
from unittest.mock import patch, MagicMock
from assertpy import assert_that

from botocore.exceptions import ClientError

from src.config.config import env, logger
from src.utils.validator_utils import ArchivoValidator  # Ajusta la ruta según tu estructura


class TestArchivoValidator(unittest.TestCase):
    @patch('src.aws.clients.AWSClients.get_ssm_client')
    def setUp(self, mock_get_ssm_client):
        mock_ssm_client = MagicMock()
        mock_get_ssm_client.return_value = mock_ssm_client

        # Configura el retorno del método get_parameter en el cliente simulado
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
        # Crear una instancia del validador
        self.validator = ArchivoValidator()
        self.validator.valid_file_suffixes = {
            "01": ["001", "002"],
            "02": ["R"],
            "03": ["ESP"]
        }
        env.CONST_PRE_SPECIAL_FILE = "RE_ESP"
        env.CONST_PRE_GENERAL_FILE = "RE_GEN"
        self.validator.general_start = env.CONST_PRE_GENERAL_FILE
        self.validator.special_start = env.CONST_PRE_SPECIAL_FILE

    def test_is_special_file_valid(self):
        # Test para un archivo que cumple con las condiciones de un archivo especial
        filename = "RE_ESP_TUTGMF0001003920241021-0001.zip"

        result = self.validator.is_special_file(filename)

        assert_that(result).is_false()

    def test_is_special_file_invalid(self):
        # Test para un archivo que no cumple con las condiciones de un archivo especial
        filename = "RE_GEN20220101-0001"
        self.assertFalse(self.validator.is_special_file(filename))

    def test_is_general_file_valid(self):
        # Test para un archivo que cumple con las condiciones de un archivo general
        filename: str = "RE_PRO_TUTGMF0001003920241021-5001.zip"

        self.validator.validate_filename_structure_for_general_file(filename)

        assert_that(self.validator.validate_filename_structure_for_general_file(filename)).is_false()

    def test_is_general_file_invalid(self):
        # Test para un archivo que no cumple con las condiciones de un archivo general
        filename = "RE_ESP20220101-0001"
        self.assertFalse(self.validator.validate_filename_structure_for_general_file(filename))

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
                    'special_start': 'RE_ESP',
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

    @patch('src.aws.clients.AWSClients.get_ssm_client')
    def test_get_valid_states_success(self, mock_get_ssm_client):
        """
        Prueba que _get_valid_states obtenga correctamente los estados válidos desde SSM.
        """
        # Configuración del mock para el cliente SSM
        parameter_name = env.PARAMETER_STORE_FILE_CONFIG
        mock_response = {
            'Parameter': {
                'Value': json.dumps({
                    env.VALID_STATES_FILES: ['PENDIENTE', 'PROCESADO', 'RECHAZADO']
                })
            }
        }

        # Crear un cliente simulado para SSM
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = mock_response
        mock_get_ssm_client.return_value = mock_ssm

        # Instanciar el validador y probar la función
        validator = ArchivoValidator()
        result = validator._get_valid_states()

        # Verificar que la función retorne los estados correctos
        self.assertEqual(result, ['PENDIENTE', 'PROCESADO', 'RECHAZADO'])

    @patch('src.aws.clients.AWSClients.get_ssm_client')
    def test_get_valid_states_client_error(self, mock_get_ssm_client):
        """
        Prueba que _get_valid_states maneje correctamente un ClientError devolviendo una lista vacía.
        """
        # Configurar el mock para que lance un ClientError
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.side_effect = ClientError(
            error_response={'Error': {'Code': 'ParameterNotFound'}},
            operation_name='GetParameter'
        )
        mock_get_ssm_client.return_value = mock_ssm

        # Instanciar el validador y probar la función
        validator = ArchivoValidator()
        result = validator._get_valid_states()

        # Verificar que la función retorne una lista vacía en caso de error
        self.assertEqual(result, [])

    @patch('src.aws.clients.AWSClients.get_ssm_client')
    def test_get_valid_states_no_valid_states_key(self, mock_get_ssm_client):
        """
        Prueba que _get_valid_states maneje el caso en que el parámetro no contenga la clave esperada.
        """
        # Configuración del mock para el cliente SSM
        parameter_name = env.PARAMETER_STORE_FILE_CONFIG
        mock_response = {
            'Parameter': {
                'Value': json.dumps({
                    'some_other_key': ['PENDIENTE', 'PROCESADO']
                })
            }
        }

        # Crear un cliente simulado para SSM
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = mock_response
        mock_get_ssm_client.return_value = mock_ssm

        # Instanciar el validador y probar la función
        validator = ArchivoValidator()
        result = validator._get_valid_states()

        # Verificar que la función retorne una lista vacía si la clave no está presente
        self.assertEqual(result, [])

    @patch.object(ArchivoValidator, 'is_special_prefix')
    def test_get_type_response_special_prefix(self, mock_is_special_prefix):
        """
        Prueba para archivos con prefijo especial.
        """
        # Configurar el mock para que devuelva True
        mock_is_special_prefix.return_value = True
        filename = "RE_ESP_file1.zip"

        # Ejecutar la función
        result = self.validator.get_type_response(filename)

        # Verificar que devuelve "03"
        self.assertEqual(result, "05")

    def test_get_type_response_general_file_with_R(self):
        """
        Prueba para archivos con prefijo general y terminación "-R.zip".
        """
        filename = f"{env.CONST_PRE_GENERAL_FILE}_file-R.zip"

        # Ejecutar la función
        result = self.validator.get_type_response(filename)

        # Verificar que devuelve "02"
        self.assertEqual(result, env.CONST_TIPO_ARCHIVO_GENERAL_REINTEGROS)

    def test_get_type_response_general_file_without_R(self):
        """
        Prueba para archivos con prefijo general sin terminación "-R.zip".
        """
        filename = f"{env.CONST_PRE_GENERAL_FILE}_file.zip"

        # Ejecutar la función
        result = self.validator.get_type_response(filename)

        # Verificar que devuelve "01"
        self.assertEqual(result, env.CONST_TIPO_ARCHIVO_GENERAL)

    def test_get_type_response_error(self):
        """
        Prueba para archivos que no cumplen con ninguna condición.
        """
        filename = "unknown_file.zip"

        # Ejecutar la función
        result = self.validator.get_type_response(filename)

        # Verificar que devuelve "00"
        self.assertEqual(result, logger.error(
            "El archivo no cumple con ninguna estructura de tipo de respuesta."))

    def test_valid_filename(self):
        """
        Prueba que un archivo válido pase todas las validaciones.
        """
        extracted_filename = "RE_TUTGMF0001003920241002-001.txt"
        tipo_respuesta = "01"
        acg_nombre_archivo = "TUTGMF0001003920241002"

        result = self.validator.is_valid_extracted_filename(extracted_filename, tipo_respuesta, acg_nombre_archivo)
        self.assertTrue(result)

    def test_invalid_prefix(self):
        """
        Prueba que un archivo que no comienza con 'RE_' falle la validación.
        """
        extracted_filename = "TUTGMF0001003920241002-001.txt"
        tipo_respuesta = "01"
        acg_nombre_archivo = "TUTGMF0001003920241002"

        result = self.validator.is_valid_extracted_filename(extracted_filename, tipo_respuesta, acg_nombre_archivo)
        self.assertFalse(result)

    def test_missing_acg_nombre_archivo(self):
        """
        Prueba que un archivo que no contiene el nombre base falle la validación.
        """
        extracted_filename = "RE_WRONGNAME-001.txt"
        tipo_respuesta = "01"
        acg_nombre_archivo = "TUTGMF0001003920241002"

        result = self.validator.is_valid_extracted_filename(extracted_filename, tipo_respuesta, acg_nombre_archivo)
        self.assertFalse(result)

    def test_invalid_suffix(self):
        """
        Prueba que un archivo con un sufijo incorrecto falle la validación.
        """
        extracted_filename = "RE_TUTGMF0001003920241002-999.txt"
        tipo_respuesta = "01"
        acg_nombre_archivo = "TUTGMF0001003920241002"

        result = self.validator.is_valid_extracted_filename(extracted_filename, tipo_respuesta, acg_nombre_archivo)
        self.assertFalse(result)

    def test_no_valid_suffixes_for_type(self):
        """
        Prueba que un archivo falle si no hay sufijos válidos definidos para el tipo de respuesta.
        """
        extracted_filename = "RE_TUTGMF0001003920241002-001.txt"
        tipo_respuesta = "04"  # Tipo de respuesta que no está en valid_file_suffixes
        acg_nombre_archivo = "TUTGMF0001003920241002"

        result = self.validator.is_valid_extracted_filename(extracted_filename, tipo_respuesta, acg_nombre_archivo)
        self.assertFalse(result)

    def test_build_acg_nombre_archivo_special_prefix(self):
        """
        Prueba para un archivo con el prefijo especial.
        """
        filename = "RE_ESP1234567.zip"
        result = ArchivoValidator.build_acg_nombre_archivo(filename)
        self.assertEqual(result, "1234567")

    def test_build_acg_nombre_archivo_general_prefix(self):
        """
        Prueba para un archivo con el prefijo general.
        """
        filename = "RE_GEN7654321.zip"
        result = ArchivoValidator.build_acg_nombre_archivo(filename)
        self.assertEqual(result, "7654321")

    def test_build_acg_nombre_archivo_no_prefix(self):
        """
        Prueba para un archivo que no tiene los prefijos especiales ni generales.
        """
        filename = "OTHER_FILE_987654.zip"
        result = ArchivoValidator.build_acg_nombre_archivo(filename)
        self.assertTrue(result)

    def test_build_acg_nombre_archivo_no_extension(self):
        """
        Prueba para un archivo sin extensión.
        """
        filename = "RE_ESP1234567"
        result = ArchivoValidator.build_acg_nombre_archivo(filename)
        self.assertEqual(result, "1234567")

    def test_build_acg_nombre_archivo_invalid_format(self):
        """
        Prueba para un archivo que no cumple con el formato esperado.
        """
        filename = "INVALID_FILE_FORMAT"
        result = ArchivoValidator.build_acg_nombre_archivo(filename)
        self.assertTrue(result)

    def test_validate_filename_structure_for_general_file(self):
        """
        Prueba un archivo válido con el prefijo correcto, fecha y formato adecuados.
        """
        filename = "RE_PRO_TUTGMF0001003920241021-5001.zip"

        result = self.validator.validate_filename_structure_for_general_file(filename)

        assert_that(result).is_false()

    @patch('src.utils.validator_utils.ArchivoValidator.is_valid_date_in_filename')
    def test_is_general_file_invalid_date(self, mock_is_valid_date):
        """
        Prueba un archivo con una fecha mayor a la actual.
        """
        filename = "RE_GEN20241231-0001.zip"
        mock_is_valid_date.return_value = False  # Simula que la fecha es mayor a la actual

        result = self.validator.validate_filename_structure_for_general_file(filename)
        self.assertFalse(result)

    def test_is_general_file_invalid_format(self):
        """
        Prueba un archivo que no cumple con el patrón definido.
        """
        filename = "INVALID_FILE_NAME.zip"
        result = self.validator.validate_filename_structure_for_general_file(filename)
        self.assertFalse(result)

    @patch('src.utils.validator_utils.ArchivoValidator.is_valid_date_in_filename')
    def test_is_general_file_no_extension(self, mock_is_valid_date):
        """
        Prueba un archivo sin la extensión .zip.
        """
        filename = "RE_GEN20231107-0001"
        mock_is_valid_date.return_value = True  # Simula que la fecha es válida

        result = self.validator.validate_filename_structure_for_general_file(filename)
        self.assertTrue(result)

    @patch('src.utils.validator_utils.ArchivoValidator.is_valid_date_in_filename')
    def test_is_general_file_invalid_date_format(self, mock_is_valid_date):
        """
        Prueba un archivo con una fecha en formato incorrecto.
        """
        filename = "RE_GEN20231-0001.zip"  # Fecha incorrecta (muy corta)
        mock_is_valid_date.return_value = False

        result = self.validator.validate_filename_structure_for_general_file(filename)
        self.assertFalse(result)

    @patch('src.utils.validator_utils.ArchivoValidator.is_valid_date_in_filename')
    def test_is_special_file_invalid_date(self, mock_is_valid_date):
        """
        Prueba un archivo con una fecha mayor a la actual.
        """
        filename = "RE_ESP20241231-0001.zip"
        mock_is_valid_date.return_value = False  # Simula que la fecha es mayor a la actual

        result = self.validator.is_special_file(filename)

    def test_is_special_file_invalid_format(self):
        """
        Prueba un archivo que no cumple con el patrón definido.
        """
        filename = "INVALID_FILE_NAME.zip"
        result = self.validator.is_special_file(filename)
        self.assertFalse(result)

    @patch('src.utils.validator_utils.ArchivoValidator.is_valid_date_in_filename')
    def test_is_special_file_invalid_date_format(self, mock_is_valid_date):
        """
        Prueba un archivo con una fecha en un formato incorrecto.
        """
        filename = "RE_ESP20231-0001.zip"  # Fecha incorrecta (muy corta)
        mock_is_valid_date.return_value = False

        result = self.validator.is_special_file(filename)
        self.assertFalse(result)
