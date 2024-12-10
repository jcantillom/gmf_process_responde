import sys
from datetime import datetime
from io import BytesIO
from zipfile import ZipFile, BadZipFile
from botocore.exceptions import ClientError
from src.services.aws_clients_service import AWSClients
from src.utils.logger_utils import get_logger
from src.config.config import env
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository
from sqlalchemy.orm import Session
from src.repositories.archivo_repository import ArchivoRepository
from src.core.validator import ArchivoValidator
from src.services.cgd_rta_pro_archivo_service import CGDRtaProArchivosService
from src.core.custom_error import CustomFunctionError
from zoneinfo import ZoneInfo


class S3Utils:
    """
    Clase para manejar operaciones de archivos en S3, incluyendo verificación de existencia,
    movimiento de archivos a carpetas de 'Rechazados' y 'Recibidos'.
    """

    def __init__(self, db: Session):
        self.s3 = AWSClients.get_s3_client()
        self.logger = get_logger(env.DEBUG_MODE)
        self.rta_procesamiento_repository = RtaProcesamientoRepository(db)
        self.archivo_repository = ArchivoRepository(db)
        self.validator = ArchivoValidator()
        self.cgd_rta_pro_archivos_service = CGDRtaProArchivosService(db)

    def check_file_exists_in_s3(self, bucket_name: str, file_key: str) -> bool:
        """
        Verifica si un archivo existe en el bucket de S3.
        """
        try:
            self.s3.head_object(Bucket=bucket_name, Key=file_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            raise

    def move_file_to_rechazados(self, bucket_name: str, source_key: str) -> str:
        """
        Mueve el archivo a la carpeta 'Rechazados/AAAAMM/' dentro del mismo bucket,
        organizado por año y mes.
        """

        current_date = datetime.now()
        year_month_folder = current_date.strftime("%Y%m")
        destination_key = f"{env.DIR_REJECTED_FILES}/{year_month_folder}/{source_key.rsplit('/', 1)[-1]}"

        # Verificar si el archivo existe antes de moverlo
        if not self.check_file_exists_in_s3(bucket_name, source_key):
            self.logger.error(
                f"El archivo {source_key} no existe en el bucket {bucket_name}. No se puede mover a Rechazados.",
                extra={"event_filename": source_key.replace(env.DIR_RECEPTION_FILES + "/", "")})
            sys.exit(1)

        try:
            self.s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': source_key},
                Key=destination_key
            )
            self.logger.debug("Archivo movido a la carpeta Rechazados",
                              extra={"event_filename": source_key.replace(env.DIR_RECEPTION_FILES + "/", "")})

            self.s3.delete_object(Bucket=bucket_name, Key=source_key)
            self.logger.debug("Archivo original eliminado",
                              extra={"event_filename": source_key.replace(env.DIR_RECEPTION_FILES + "/", "")})

            return destination_key

        except ClientError as e:
            self.logger.error("Error al mover el archivo a Rechazados: %s", e)
            sys.exit(1)

    def move_file_to_procesando(self, bucket_name: str, file_name: str) -> str:
        """
        Mueve el archivo a la carpeta 'Procesando/YYYYMM/' dentro del mismo bucket,
        :returns: La clave de destino del archivo.
        """
        source_key = f"{env.DIR_RECEPTION_FILES}/{file_name}"

        # Crear la nueva clave de destino con la carpeta de año y mes
        destination_key = f"{env.DIR_PROCESSING_FILES}/{file_name}"

        try:
            # Copiar el archivo a la nueva ubicación
            self.s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': source_key},
                Key=destination_key
            )
            self.logger.debug("Archivo movido a la carpeta Procesando",
                              extra={"event_filename": file_name})

            # Eliminar el archivo original de la carpeta Recibidos
            self.s3.delete_object(Bucket=bucket_name, Key=source_key)
            self.logger.debug("Archivo original eliminado",
                              extra={"event_filename": file_name})

            return destination_key

        except ClientError as e:
            self.logger.error("Error al mover el archivo a Procesando: %s", e)
            sys.exit(1)

    def unzip_file_in_s3(
            self,
            bucket_name: str,
            file_key: str,
            id_archivo: int,
            nombre_archivo: str,
            receipt_handle: str,
            error_handling_service
    ):
        """
        Descomprime un archivo .zip en S3 y sube el contenido descomprimido a una carpeta
        con el nombre del archivo y un timestamp actual.

        :param bucket_name: El nombre del bucket de S3.
        :param file_key: La clave del archivo .zip en S3.
        :param id_archivo: El ID del archivo en la base de datos.
        :param nombre_archivo: El nombre del archivo en la base de datos.
        :param contador_intentos_cargue: El contador de intentos de cargue del archivo.
        :param receipt_handle: El identificador del mensaje en la cola de SQS.
        :param error_handling_service: Instancia de ErrorHandlingService para manejar errores.
        """

        # Obtener el tipo de respuesta del archivo.
        expected_file_count, tipo_respuesta = (
            self.get_cantidad_de_archivos_esperados_en_el_zip(id_archivo, nombre_archivo))

        colombia_tz = ZoneInfo("America/Bogota")
        timestamp = datetime.now(colombia_tz).strftime("%Y%m%d%H%M%S")
        base_folder = file_key.rsplit("/", 1)[0]
        zip_filename = file_key.rsplit("/", 1)[-1].replace('.zip', '')
        destination_folder = f"{base_folder}/{zip_filename}_{timestamp}/"

        try:
            zip_obj = self.s3.get_object(Bucket=bucket_name, Key=file_key)
            with ZipFile(BytesIO(zip_obj['Body'].read())) as zip_file:
                extracted_files = []
                for file_info in zip_file.infolist():
                    # omitir carpetas
                    if file_info.is_dir():
                        continue
                    # Crear la clave S3 para cada archivo descomprimido
                    extracted_file_key = f"{destination_folder}{file_info.filename}"
                    extracted_files.append(extracted_file_key)

                # validar si la cantidad de archivos descomprimidos es igual a la cantidad esperada
                contador_archivos_descomprimidos = self.validar_cantidad_archivos_descomprimidos(
                    extracted_files, expected_file_count
                )

                # si la cantidad de archivos descomprimidos no es igual a la cantidad esperada
                if not contador_archivos_descomprimidos:
                    error_handling_service.handle_generic_error(
                        id_archivo=id_archivo,
                        filekey=file_key,
                        bucket_name=bucket_name,
                        receipt_handle=receipt_handle,
                        file_name=nombre_archivo,
                        codigo_error=env.CONST_COD_ERROR_UNEXPECTED_FILE_COUNT,
                        id_plantilla=env.CONST_ID_PLANTILLA_CORREO_ERROR_DECOMPRESION,
                    )
                    raise CustomFunctionError(
                        code=env.CONST_COD_ERROR_UNEXPECTED_FILE_COUNT,
                        error_details="La cantidad de archivos descomprimidos no es igual a la cantidad esperada",
                        is_technical_error=False,
                    )

                    return

                # validar la estructura del nombre de cada archivo descomprimido
                for file_info in zip_file.infolist():
                    is_valid = self.validator.validar_archivos_in_zip(
                        file_info.filename, tipo_respuesta, nombre_archivo
                    )
                    if not is_valid:
                        # nombre con extensión .zip para el log.
                        nombre_archivo_zip = nombre_archivo + ".zip"
                        self.logger.error(
                            "Nombre del archivo descomprimido no cumple con la estructura esperada: ",
                            extra={"event_filename": nombre_archivo_zip}
                        )
                        # handling error
                        error_handling_service.handle_generic_error(
                            id_archivo=id_archivo,
                            filekey=file_key,
                            bucket_name=bucket_name,
                            receipt_handle=receipt_handle,
                            file_name=nombre_archivo,
                            codigo_error=env.CONST_COD_ERROR_INVALID_FILE_SUFFIX,
                            id_plantilla=env.CONST_ID_PLANTILLA_CORREO_ERROR_DECOMPRESION,
                        )

                    else:
                        # Leer el contenido del archivo extraído
                        with zip_file.open(file_info) as extracted_file:
                            # Subir el archivo descomprimido a S3
                            self.logger.debug(
                                f"Archivos extraídos: {[file_info.filename for file_info in zip_file.infolist()]}")
                            extracted_file_key = f"{destination_folder}{file_info.filename}"
                            self.s3.upload_fileobj(
                                extracted_file,
                                Bucket=bucket_name,
                                Key=extracted_file_key
                            )
                            self.logger.debug(f"Archivo descomprimido subido a S3: {extracted_file_key}")

            # Eliminar el archivo .zip original
            self.s3.delete_object(Bucket=bucket_name, Key=file_key)
            self.logger.debug(f"Archivo .zip original eliminado: {file_key}")
            self.logger.info(f"Descompresión completada para {file_key} en {destination_folder}")

            # Registrar los archivos descomprimidos en la tabla CGD_RTA_PRO_ARCHIVOS
            zip_filename = file_key.rsplit("/", 1)[-1]
            id_rta_procesamiento = self.rta_procesamiento_repository.get_last_rta_procesamiento_without_archivos(
                id_archivo,
                zip_filename,
            )
            # Registrar los archivos descomprimidos en la tabla CGD_RTA_PRO_ARCHIVOS
            self.cgd_rta_pro_archivos_service.register_extracted_files(
                id_archivo=id_archivo,
                id_rta_procesamiento=id_rta_procesamiento,
                extracted_files=extracted_files,
            )

            # Enviar mensajes a la cola para cada archivo descomprimido
            self.cgd_rta_pro_archivos_service.send_pending_files_to_queue_by_id(
                id_archivo=id_archivo,
                queue_url=env.SQS_URL_PRO_RESPONSE_TO_UPLOAD,
                destination_folder=destination_folder,
            )
            return destination_folder

        except (ConnectionError, IOError) as e:
            self.logger.error(f"Error técnico al descomprimir el archivo {nombre_archivo}: {str(e)}",
                              extra={"event_filename": nombre_archivo})
            error_handling_service.handle_generic_error(
                id_archivo=id_archivo,
                filekey=file_key,
                bucket_name=bucket_name,
                receipt_handle=receipt_handle,
                file_name=nombre_archivo,
                codigo_error=env.CONST_COD_ERROR_TECHNICAL_UNZIP,
                id_plantilla=env.CONST_ID_PLANTILLA_CORREO_ERROR_DECOMPRESION,

            )
            raise CustomFunctionError(
                code=env.CONST_COD_ERROR_TECHNICAL_UNZIP,
                message="Error técnico al descomprimir el archivo",
                is_technical_error=True,
            )

        except BadZipFile:
            error_handling_service.handle_generic_error(
                id_archivo=id_archivo,
                filekey=file_key,
                bucket_name=bucket_name,
                receipt_handle=receipt_handle,
                file_name=nombre_archivo,
                codigo_error=env.CONST_COD_ERROR_CORRUPTED_FILE,
                id_plantilla=env.CONST_ID_PLANTILLA_CORREO_ERROR_DECOMPRESION,
            )
            self.logger.error("Error al descomprimir el archivo .zip", extra={"event_filename": nombre_archivo})
            raise CustomFunctionError(
                code=env.CONST_COD_ERROR_CORRUPTED_FILE,
                error_details=".zip es inválido o está corrupto",
                is_technical_error=False,
            )
            return None

    def get_cantidad_de_archivos_esperados_en_el_zip(self, id_archivo, nombre_archivo):
        tipo_respuesta = self.rta_procesamiento_repository.get_tipo_respuesta(id_archivo)
        expected_file_count = {
            "01": 5,
            "02": 3,
            "03": 2,
        }.get(tipo_respuesta, None)

        if expected_file_count is None:
            self.logger.error(f" La cantidad de archivos esperados para el tipo de respuesta {tipo_respuesta} "
                              f"no es válida.", extra={"event_filename": nombre_archivo})
            sys.exit(1)

        # retornar expected_file_count y tipo_respuesta
        return expected_file_count, tipo_respuesta

    def validar_cantidad_archivos_descomprimidos(self, extracted_files, expected_file_count):
        if len(extracted_files) != expected_file_count:
            self.logger.error(
                "La cantidad de archivos descomprimidos no es igual a la cantidad esperada: " + str(
                    len(extracted_files)) + " vs " + str(expected_file_count))
            return False
        return True

    # function para validar si hay archivos descomprimidos en la carpeta de procesando
    def validate_decompressed_files_in_processing(
            self,
            bucket_name: str,
            processing_folder: str,
            archivo_base: str
    ):
        """
        Valida si existen archivos descomprimidos en la carpeta 'Procesando' de S3
        que correspondan al archivo base.

        :param bucket_name: Nombre del bucket en S3.
        :param processing_folder: Nombre de la carpeta 'Procesando' en el bucket.
        :param archivo_base: Nombre base del archivo (sin extensión).
        :return: True si la carpeta existe y contiene archivos, False si no hay carpeta o está vacía.
        """
        try:
            archivo_base = archivo_base.rsplit(".zip", 1)[0]
            # Generar el prefijo para buscar carpetas que contengan el nombre base del archivo
            folder_prefix = f"{processing_folder}/{archivo_base}_"

            # Depuración: imprimir prefijo que se está buscando
            self.logger.debug(f"Buscando carpetas con prefijo: {folder_prefix}")
            # Listar las carpetas en 'Procesando'
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

            # Depuración: imprimir respuesta completa de AWS S3
            self.logger.debug(f"Respuesta S3: {response}")

            # Verificar si hay objetos con el prefijo
            if 'Contents' in response and len(response['Contents']) > 0:
                self.logger.debug(f"Archivos encontrados: {[obj['Key'] for obj in response['Contents']]}")
                return True

            # Si no hay contenido, significa que no existen carpetas o están vacías
            self.logger.debug(f"No se encontraron archivos con el prefijo {folder_prefix}.")
            return False

        except Exception as e:
            self.logger.error(
                f"Error al validar archivos descomprimidos para {archivo_base} en '{processing_folder}': {str(e)}")
            return False
