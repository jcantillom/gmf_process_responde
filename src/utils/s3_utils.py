from datetime import datetime
from io import BytesIO
from zipfile import ZipFile, BadZipFile
from botocore.exceptions import ClientError
from src.aws.clients import AWSClients
from src.logs.logger import get_logger
from src.config.config import env
from src.repositories.rta_procesamiento_repository import RtaProcesamientoRepository
from sqlalchemy.orm import Session
from src.repositories.archivo_repository import ArchivoRepository


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
        Mueve el archivo a la carpeta 'Rechazados/AAAA/MM/' dentro del mismo bucket,
        organizado por año y mes.
        """
        current_date = datetime.now()
        year_month_folder = current_date.strftime("%Y%m")
        destination_key = source_key.replace(
            env.DIR_RECEPTION_FILES,
            f"{env.DIR_REJECTED_FILES}/{year_month_folder}")

        try:
            self.s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': source_key},
                Key=destination_key
            )
            self.logger.info("Archivo movido a la carpeta Rechazados: %s", destination_key)

            self.s3.delete_object(Bucket=bucket_name, Key=source_key)
            self.logger.info("Archivo original eliminado: %s", source_key)

            return destination_key

        except ClientError as e:
            self.logger.error("Error al mover el archivo a Rechazados: %s", e)

    def move_file_to_procesando(self, bucket_name: str, source_key: str) -> str:
        """
        Mueve el archivo a la carpeta 'Procesando/AAAAMM/' dentro del mismo bucket,
        organizado por año y mes.
        :returns: La clave de destino del archivo.
        """
        current_date = datetime.now()
        year_month_folder = current_date.strftime("%Y%m")
        destination_key = source_key.replace(
            env.DIR_RECEPTION_FILES,
            f"{env.DIR_PROCESSING_FILES}/{year_month_folder}")
        try:
            self.s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': source_key},
                Key=destination_key
            )
            self.logger.info("Archivo movido a la carpeta Procesando: %s", destination_key)

            self.s3.delete_object(Bucket=bucket_name, Key=source_key)
            self.logger.debug("Archivo original eliminado: %s", source_key)

            return destination_key



        except ClientError as e:
            self.logger.error("Error al mover el archivo a Procesando: %s", e)

    def unzip_file_in_s3(
            self,
            bucket_name: str,
            file_key: str,
            id_archivo: int,
            nombre_archivo: str,
            contador_intentos_cargue: int,
            receipt_handle: str,
            error_handling_service
    ) -> None:
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
        tipo_respuesta = self.rta_procesamiento_repository.get_tipo_respuesta(id_archivo)
        expected_file_count = {
            "01": 5,
            "02": 3,
            "03": 2,
        }.get(tipo_respuesta, None)

        if expected_file_count is None:
            self.logger.error(f"Tipo de respuesta no válido: {tipo_respuesta}")
            return

        original_file_key = file_key
        file_key = file_key.replace(env.DIR_PROCESSING_FILES, env.DIR_RECEPTION_FILES)

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        base_folder = original_file_key.rsplit("/", 1)[0]  # Extrae la carpeta base (Procesando/AAAA/MM)
        zip_filename = original_file_key.rsplit("/", 1)[-1].replace('.zip', '')
        destination_folder = f"{base_folder}/{zip_filename}_{timestamp}/"

        try:
            zip_obj = self.s3.get_object(Bucket=bucket_name, Key=original_file_key)
            self.logger.debug(f"zip_obj: {zip_obj}")

            with ZipFile(BytesIO(zip_obj['Body'].read())) as zip_file:
                extracted_files = []
                for file_info in zip_file.infolist():
                    # Crear la clave S3 para cada archivo descomprimido
                    extracted_file_key = f"{destination_folder}{file_info.filename}"
                    extracted_files.append(extracted_file_key)

                    # Leer el contenido del archivo extraído
                    with zip_file.open(file_info) as extracted_file:
                        # Subir el archivo descomprimido a S3
                        self.s3.upload_fileobj(
                            extracted_file,
                            Bucket=bucket_name,
                            Key=extracted_file_key
                        )
                        self.logger.debug(f"Archivo descomprimido subido: {extracted_file_key}")

            # validacion de cantidad de archivos descomprimidos
            if len(extracted_files) != expected_file_count:
                self.logger.error(
                    f"Error en el .zip descomprimido: {file_key}. "
                    f"Se esperaban {expected_file_count} archivos, "
                    f"pero se encontraron {len(extracted_files)}."
                )
                # # actualizar el estado de CGD_RTA_PROCESAMIENTO a RECHAZADO
                # self.rta_procesamiento_repository.update_state_rta_procesamiento(
                #     id_archivo=id_archivo,
                #     estado=env.CONST_ESTADO_REJECTED
                # )
                #
                # # actualizar el estado del archivo a PROCESAMIENTO_RECHAZADO
                # self.archivo_repository.update_estado_archivo(
                #     nombre_archivo,
                #     env.CONST_ESTADO_PROCESAMIENTO_RECHAZADO,
                #     contador_intentos_cargue=contador_intentos_cargue,
                # )
                #
                # # llamamos al error_handling para enviar el mensaje de error
                # error_handling_service.handle_file_error(
                #     id_plantilla=env.CONST_ID_PLANTILLA_CORREO_ERROR_DECOMPRESION,
                #     filekey=file_key,
                #     bucket=bucket_name,
                #     receipt_handle=receipt_handle,
                #     codigo_error=env.CONST_COD_ERROR_DECOMPRESION,
                #     filename=nombre_archivo,
                # )

                # handling error
                error_handling_service.handle_unzip_error(
                    id_archivo=id_archivo,
                    nombre_archivo=nombre_archivo,
                    contador_intentos_cargue=contador_intentos_cargue,
                    file_key=file_key,
                    bucket_name=bucket_name,
                    receipt_handle=receipt_handle,
                )

            # Eliminar el archivo .zip original
            self.s3.delete_object(Bucket=bucket_name, Key=original_file_key)
            self.logger.debug(f"Archivo .zip original eliminado: {original_file_key}")
            self.logger.info(f"Descompresión completada para {file_key} en {destination_folder}")

        except BadZipFile:
            # # actualizar el estado de CGD_RTA_PROCESAMIENTO a RECHAZADO
            # self.rta_procesamiento_repository.update_state_rta_procesamiento(
            #     id_archivo=id_archivo,
            #     estado=env.CONST_ESTADO_REJECTED
            # )
            #
            # # actualizar el estado del archivo a PROCESAMIENTO_RECHAZADO
            # self.archivo_repository.update_estado_archivo(
            #     nombre_archivo,
            #     env.CONST_ESTADO_PROCESAMIENTO_RECHAZADO,
            #     contador_intentos_cargue=contador_intentos_cargue,
            # )
            #
            # # llamamos al error_handling para enviar el mensaje de error
            # error_handling_service.handle_file_error(
            #     id_plantilla=env.CONST_ID_PLANTILLA_CORREO_ERROR_DECOMPRESION,
            #     filekey=file_key,
            #     bucket=bucket_name,
            #     receipt_handle="",
            #     codigo_error=env.CONST_COD_ERROR_DECOMPRESION,
            #     filename=nombre_archivo,
            # )

            # handling error
            error_handling_service.handle_unzip_error(
                id_archivo=id_archivo,
                nombre_archivo=nombre_archivo,
                contador_intentos_cargue=contador_intentos_cargue,
                file_key=file_key,
                bucket_name=bucket_name,
                receipt_handle=receipt_handle,
            )

            self.logger.error(f"Error al descomprimir el archivo {file_key}: Archivo .zip inválido.")
