import re
import json
from datetime import datetime
from botocore.exceptions import ClientError
from src.aws.clients import AWSClients
from src.logs.logger import get_logger
from src.config.config import env

logger = get_logger(env.DEBUG_MODE)


class ArchivoValidator:
    """
    Clase para validar archivos en el sistema.
    """

    def __init__(self):
        # Inicializa el cliente SSM solo una vez
        self.ssm_client = AWSClients.get_ssm_client()
        self.special_start, self.special_end, self.general_start = self._get_file_config_name()

    def _get_file_config_name(self):
        """
        Obtiene la configuración de nombres de archivos especiales desde Parameter Store.
        """
        parameter_name = env.PARAMETER_STORE_FILE_CONFIG

        try:
            response = self.ssm_client.get_parameter(Name=parameter_name)
            parameter_data = json.loads(response['Parameter']['Value'])

            special_start = parameter_data.get(env.SPECIAL_START_NAME, "")
            special_end = parameter_data.get(env.SPECIAL_END_NAME, "")
            general_start = parameter_data.get(env.GENERAL_START_NAME, "")

            return special_start, special_end, general_start

        except ClientError as e:
            logger.error(f"Error al obtener el parámetro {parameter_name}: {e}")
            return "", "", ""

    @staticmethod
    def is_special_prefix(filename: str) -> bool:
        """
        Verifica si el archivo tiene el prefijo especial.
        """
        return filename.startswith(env.CONST_PRE_SPECIAL_FILE)

    @staticmethod
    def is_valid_date_in_filename(fecha_str: str) -> bool:
        """
        Verifica que la fecha extraída del nombre del archivo no sea mayor a la fecha actual.

        Args:
            fecha_str (str): Fecha en formato YYYYMMDD extraída del nombre del archivo.

        Returns:
            bool: True si la fecha es válida y no es mayor a la fecha actual, False en caso contrario.
        """
        try:
            fecha_archivo = datetime.strptime(fecha_str, "%Y%m%d")
            return fecha_archivo <= datetime.now()
        except ValueError:
            logger.error(f"Error en el formato de fecha {fecha_str}.")
            return False

    def is_special_file(self, filename: str) -> bool:
        """
        Verifica si el archivo cumple con la estructura definida para archivos especiales
        y que la fecha en el nombre no sea mayor a la fecha actual.
        """
        # Remueve el sufijo .zip si está presente
        if filename.endswith(".zip"):
            filename = filename[:-4]

        # Definir el patrón de archivo especial
        expected_pattern = f"^{self.special_start}(\\d{{8}})-{self.special_end}$"

        match = re.match(expected_pattern, filename)
        if not match:
            logger.debug(f"El archivo {filename} no cumple con la estructura de un archivo especial.")
            return False

        # Extraer la fecha del nombre de archivo y validar que no sea mayor a la fecha actual
        fecha_str = match.group(1)
        if not self.is_valid_date_in_filename(fecha_str):
            logger.debug(f"La fecha {fecha_str} en el archivo {filename} es mayor a la fecha actual.")
            return False

        logger.debug(f"El archivo {filename} cumple con la estructura de archivo especial.")
        return True

    def is_general_file(self, filename: str) -> bool:
        """
        Verifica si el archivo cumple con la estructura definida para archivos generales.
        """
        # Remueve el sufijo .zip si está presente
        if filename.endswith(".zip"):
            filename = filename[:-4]

        # Definir el patrón de archivo general
        expected_pattern = f"^{self.general_start}(\\d{{8}})-\\d{{4}}$"

        match = re.match(expected_pattern, filename)
        if not match:
            logger.debug(f"El archivo {filename} no cumple con la estructura de un archivo general.")
            return False

        # Extraer la fecha del nombre de archivo y validar que no sea mayor a la fecha actual
        fecha_str = match.group(1)
        if not self.is_valid_date_in_filename(fecha_str):
            logger.debug(f"La fecha {fecha_str} en el archivo {filename} es mayor a la fecha actual.")
            return False

        logger.debug(f"El archivo {filename} cumple con la estructura de archivo general.")
        return True

    def build_acg_nombre_archivo(self, filename: str) -> str:
        """
        Construye el acg_nombre_archivo sin el prefijo ni la extensión.
        """
        # Remueve el prefijo y la extensión .zip
        filename_without_ext = filename.rsplit(".", 1)[0]

        if (filename_without_ext.startswith(env.CONST_PRE_SPECIAL_FILE) or
                filename_without_ext.startswith(env.CONST_PRE_GENERAL_FILE)):
            return filename_without_ext[7:]

    def _get_valid_states(self) -> list[str]:
        """
        Obtiene los estados válidos para los archivos.
        """
        parameter_name = env.PARAMETER_STORE_FILE_CONFIG

        try:
            response = self.ssm_client.get_parameter(Name=parameter_name)
            parameter_data = json.loads(response['Parameter']['Value'])
            valid_states = parameter_data.get(env.VALID_STATES_FILES, [])
            logger.debug(f"Cargando estados válidos: {valid_states}")
            return valid_states
        except ClientError as e:
            logger.error(f"Error al obtener el parámetro {parameter_name}: {e}")
            return []

    def is_valid_state(self, state: str) -> bool:
        """
        Verifica si el estado del archivo es válido.
        """
        valid_states = self._get_valid_states()
        return state in valid_states

    def is_not_processed_state(self, state: str) -> bool:
        """
        Verifica si el estado del archivo no es procesado.
        """
        return state != env.CONST_ESTADO_PROCESSED

    def get_type_response(self, filename: str) -> str:
        """
        Obtiene el tipo de respuesta del archivo.
        """
        if self.is_special_prefix(filename):
            return "03"
        elif filename.startswith(env.CONST_PRE_GENERAL_FILE) and filename.endswith("-R.zip"):
            return "02"
        elif filename.startswith(env.CONST_PRE_GENERAL_FILE):
            return "01"
        else:
            return "00"
