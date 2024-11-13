import sys
from datetime import datetime
from fileinput import filename
from io import BytesIO
from zipfile import ZipFile, BadZipFile
from botocore.exceptions import ClientError
from src.aws.clients import AWSClients
from src.logs.logger import get_logger
from src.config.config import env
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository
from sqlalchemy.orm import Session
from src.repositories.archivo_repository import ArchivoRepository
from src.utils.validator_utils import ArchivoValidator
from src.services.cgd_rta_pro_archivo_service import CGDRtaProArchivosService


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

    def move_file_to_procesando(self, bucket_name: str, file_name: str) -> str:
        """
        Mueve el archivo a la carpeta 'Procesando/YYYYMM/' dentro del mismo bucket,
        :returns: La clave de destino del archivo.
        """
        source_key = f"{env.DIR_RECEPTION_FILES}/{file_name}"

        # Obtener el año y mes actual para crear la ruta
        current_date = datetime.now()
        year_month_folder = current_date.strftime("%Y%m")

        # Crear la nueva clave de destino con la carpeta de año y mes
        destination_key = f"{env.DIR_PROCESSING_FILES}/{year_month_folder}/{file_name}"

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
            contador_intentos_cargue: int,
            receipt_handle: str,
            error_handling_service
    ) :
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

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        base_folder = file_key.rsplit("/", 1)[0]
        zip_filename = file_key.rsplit("/", 1)[-1].replace('.zip', '')
        destination_folder = f"{base_folder}/{zip_filename}_{timestamp}/"

        try:
            zip_obj = self.s3.get_object(Bucket=bucket_name, Key=file_key)
            with ZipFile(BytesIO(zip_obj['Body'].read())) as zip_file:
                extracted_files = []
                for file_info in zip_file.infolist():
                    # Crear la clave S3 para cada archivo descomprimido
                    extracted_file_key = f"{destination_folder}{file_info.filename}"
                    extracted_files.append(extracted_file_key)

                # validar si la cantidad de archivos descomprimidos es igual a la cantidad esperada
                contador_archivos_descomprimidos = self.validar_cantidad_archivos_descomprimidos(
                    extracted_files, expected_file_count
                )

                # si la cantidad de archivos descomprimidos no es igual a la cantidad esperada
                if not contador_archivos_descomprimidos:
                    # handling error
                    error_handling_service.handle_unexpected_file_count_error(
                        id_archivo=id_archivo,
                        filekey=file_key,
                        bucket_name=bucket_name,
                        receipt_handle=receipt_handle,
                        file_name=nombre_archivo,
                        contador_intentos_cargue=contador_intentos_cargue,
                    )

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
                        error_handling_service.handle_invalid_file_suffix_error(
                            id_archivo=id_archivo,
                            filekey=file_key,
                            bucket_name=bucket_name,
                            receipt_handle=receipt_handle,
                            file_name=nombre_archivo,
                            contador_intentos_cargue=contador_intentos_cargue,
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
            error_handling_service.handle_technical_unzip_error(
                id_archivo=id_archivo,
                filekey=file_key,
                bucket_name=bucket_name,
                receipt_handle=receipt_handle,
                file_name=nombre_archivo,
                contador_intentos_cargue=contador_intentos_cargue,
            )

        except BadZipFile:
            error_handling_service.handle_corrupted_zip_file_error(
                id_archivo=id_archivo,
                filekey=file_key,
                bucket_name=bucket_name,
                receipt_handle=receipt_handle,
                file_name=nombre_archivo,
                contador_intentos_cargue=contador_intentos_cargue,
            )
            self.logger.error("Error al descomprimir el archivo .zip", extra={"event_filename": nombre_archivo})
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
                "La cantidad de archivos descomprimidos no es igual a la cantidad esperada",
                extra={"event_filename": extracted_files}
            )
            return None
        return extracted_files
