import os.path
import re
import json
from datetime import datetime
from botocore.exceptions import ClientError
from src.services.aws_clients_service import AWSClients
from src.utils.logger_utils import get_logger
from src.config.config import env

logger = get_logger(env.DEBUG_MODE)


class ArchivoValidator:
    """
    Clase para validar archivos en el sistema.
    """

    def __init__(self):
        # Inicializa el cliente SSM solo una vez
        self.ssm_client = AWSClients.get_ssm_client()
        (
            self.special_start,
            self.special_end,
            self.general_start,
            self.valid_file_suffixes
        ) = self._get_file_config_name()

    def _get_file_config_name(self):
        """
        Obtiene la configuración de nombres de archivos especiales desde Parameter Store.
        """
        parameter_name = env.PARAMETER_STORE_FILE_CONFIG

        try:
            response = self.ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
            parameter_data = json.loads(response['Parameter']['Value'])

            special_start = parameter_data.get(env.SPECIAL_START_NAME, "")
            special_end = parameter_data.get(env.SPECIAL_END_NAME, "")
            general_start = parameter_data.get(env.GENERAL_START_NAME, "")
            valid_file_suffixes = {
                "01": parameter_data.get("files-reponses-debito-reverso", "").split(","),
                "02": parameter_data.get("files-reponses-reintegros", "").split(","),
                "03": parameter_data.get("files-reponses-especiales", "").split(",")
            }
            return special_start, special_end, general_start, valid_file_suffixes

        except ClientError as e:
            logger.error(f"Error al obtener el parámetro {parameter_name}: {e}")
            return "", "", "", {}

    @staticmethod
    def get_retry_parameters(parameter_name: str) -> dict:
        """
        Obtiene los parámetros de configuración de reintentos desde Parameter Store.
        """
        try:
            ssm_client = AWSClients.get_ssm_client()
            response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
            parameter_data = json.loads(response['Parameter']['Value'])
            return parameter_data
        except ClientError as e:
            logger.error(f"Error al obtener el parámetro  de reintento {parameter_name}: {e}")
            return {"number-retries": "5", "time-between-retry": "900"}

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

        logger.debug("El archivo cumple con la estructura de archivo especial.",
                     extra={"event_filename": {
                         "filename": filename,
                         "fecha_str": fecha_str,
                     }})
        return True

    def validate_filename_structure_for_general_file(self, filename: str) -> bool:
        """
        Verifica si el archivo cumple con la estructura del nombre definida para archivos generales.
        Ahora también acepta nombres de archivo que terminan en '-R'.
        """
        # Remueve el sufijo .zip si está presente
        if filename.endswith(".zip"):
            filename = filename[:-4]

        # Definir el patrón de archivo general, permitiendo '-R' opcionalmente
        expected_pattern = f"^{self.general_start}(\\d{{8}})-\\d{{4}}(-R)?$"

        match = re.match(expected_pattern, filename)
        if not match:
            logger.debug(f"El archivo {filename} no cumple con el patrón de estructura general: {expected_pattern}")
            return False

        # Extraer la fecha del nombre de archivo y validar que no sea mayor a la fecha actual
        fecha_str = match.group(1)
        if not self.is_valid_date_in_filename(fecha_str):
            logger.debug(f"La fecha {fecha_str} en el archivo {filename} es mayor a la fecha actual.")
            return False

        logger.debug(
            "El archivo cumple con la estructura de archivo general y la fecha en el nombre es válida.",
            extra={"event_filename": {
                "filename": filename,
                "fecha_str": fecha_str,
            }}
        )
        return True

    @staticmethod
    def build_acg_nombre_archivo(filename: str) -> str:
        """
        Construye el acg_nombre_archivo sin el prefijo ni la extensión.
        """
        # Remueve la extensión .zip
        filename_without_ext = filename.rsplit(".", 1)[0]

        # Verificar si el archivo tiene el prefijo especial
        if filename_without_ext.startswith(env.CONST_PRE_SPECIAL_FILE):
            return filename_without_ext[len(env.CONST_PRE_SPECIAL_FILE):]

        # Verificar si el archivo tiene el prefijo general
        if filename_without_ext.startswith(env.CONST_PRE_GENERAL_FILE):
            return filename_without_ext[len(env.CONST_PRE_GENERAL_FILE):]

        return filename_without_ext

    def _get_valid_states(self) -> list[str]:
        """
        Obtiene los estados válidos para los archivos.
        """
        parameter_name = env.PARAMETER_STORE_FILE_CONFIG

        try:
            response = self.ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
            parameter_data = json.loads(response['Parameter']['Value'])
            valid_states = parameter_data.get(env.VALID_STATES_FILES, [])
            logger.debug(f"estados válidos: {valid_states}")
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

    @staticmethod
    def is_not_processed_state(state: str) -> bool:
        """
        Verifica si el estado del archivo no es procesado.
        """
        return state != env.CONST_ESTADO_PROCESSED

    def get_type_response(self, filename: str) -> str:
        """
        Obtiene el tipo de respuesta del archivo.
        """
        if self.is_special_prefix(filename):
            return env.CONST_TIPO_ARCHIVO_ESPECIAL
        elif filename.startswith(env.CONST_PRE_GENERAL_FILE) and filename.endswith("-R.zip"):
            return env.CONST_TIPO_ARCHIVO_GENERAL_REINTEGROS
        elif filename.startswith(env.CONST_PRE_GENERAL_FILE):
            return env.CONST_TIPO_ARCHIVO_GENERAL
        else:
            logger.error("El archivo no cumple con ninguna estructura de tipo de respuesta.",
                         extra={"event_filename": {"filename": filename}})

    def is_valid_extracted_filename(self, extracted_filename: str, tipo_respuesta: str,
                                    acg_nombre_archivo: str) -> bool:
        """
        Valida que el nombre del archivo extraído cumpla con las reglas específicas:
        - Empieza con 'RE_'
        - Contiene el 'acg_nombre_archivo' original
        - Termina con un sufijo válido según el tipo de respuesta.
        """
        # Verificar prefijo 'RE_'
        if not extracted_filename.startswith("RE_"):
            logger.error(f"El archivo {extracted_filename} no comienza con 'RE_'.")
            return False

        # Verificar si contiene el 'acg_nombre_archivo'
        if acg_nombre_archivo not in extracted_filename:
            logger.error(f"El archivo {extracted_filename} no contiene el nombre base {acg_nombre_archivo}.")
            return False

        # Obtener el sufijo del archivo y validar contra el tipo de respuesta
        valid_suffixes = self.valid_file_suffixes.get(tipo_respuesta, [])
        logger.debug(f"Sufijos válidos para tipo {tipo_respuesta}: {valid_suffixes}")

        suffix_match = any(extracted_filename.endswith(f"-{suffix}.txt") for suffix in valid_suffixes)

        if not suffix_match:
            logger.error(
                f"El archivo {extracted_filename} no finaliza con un sufijo válido para tipo {tipo_respuesta}."
            )
            return False

        logger.debug(
            f"El archivo {extracted_filename} cumple con todas las validaciones de estructura para tipo {tipo_respuesta}."
        )
        return True

    def validar_archivos_in_zip(self, extracted_filename: str, tipo_respuesta: str,
                                acg_nombre_archivo: str) -> bool:
        """
        Valida los archivos contenidos en un archivo zip.
        """
        if not extracted_filename.startswith("RE_"):
            logger.error(f"El archivo {extracted_filename} no comienza con 'RE_'.")
            return False

        nombre_base_zip = os.path.splitext(os.path.basename(acg_nombre_archivo))[0]

        nombre_base_zip_sin_prefijo = nombre_base_zip

        # elimina el prefijo 'ESP_' o "PRO" del nombre del archivo
        if nombre_base_zip.startswith(env.CONST_PRE_SPECIAL_FILE):
            nombre_base_zip_sin_prefijo = nombre_base_zip.replace(env.CONST_PRE_SPECIAL_FILE + "_", "")
        elif nombre_base_zip.startswith(env.CONST_PRE_GENERAL_FILE):
            nombre_base_zip_sin_prefijo = nombre_base_zip.replace(env.CONST_PRE_GENERAL_FILE + "_", "")

        # veriricar si nombre_base_zip_sin_prefijo esta en extracted_filename
        if nombre_base_zip_sin_prefijo not in extracted_filename:
            logger.error(f"El archivo {extracted_filename} no contiene el nombre base {nombre_base_zip_sin_prefijo}.",
                         extra={"event_filename": {"filename": acg_nombre_archivo}})
            return False

        # verificar si el archivo tiene el sufijo correcto
        valid_suffixes = self.valid_file_suffixes.get(tipo_respuesta, [])
        logger.debug(f"Sufijos válidos para tipo {tipo_respuesta}: {valid_suffixes}",
                     extra={"event_filename": acg_nombre_archivo})

        suffix_match = any(extracted_filename.endswith(f"-{suffix}.txt") for suffix in valid_suffixes)

        if not suffix_match:
            logger.error(
                f"El archivo {extracted_filename} no finaliza con un sufijo válido para tipo {tipo_respuesta}.",
                extra={"event_filename": acg_nombre_archivo})
            return False

        logger.debug(
            f"El archivo {extracted_filename} cumple con todas las validaciones de estructura para tipo {tipo_respuesta}.",
            extra={"event_filename": acg_nombre_archivo})

        return True
