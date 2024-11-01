import os
from contextlib import contextmanager
from sqlalchemy import URL
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.session import Session
from src.aws.clients import AWSClients
from dotenv import load_dotenv
from src.config.config import env
from src.logs import logger as log

# Carga las variables de entorno
load_dotenv()

Base = declarative_base()


class DataAccessLayer:
    """
    Clase para manejar la conexión a la base de datos
    """

    def __init__(self):
        """
        Inicializa la conexión a la base de datos,
        obtiene las credenciales de Usuario y Contraseña de la base de datos desde AWS Secrets Manager,
        las otras credenciales de la base de datos se obtienen de las variables de entorno.
        """
        try:
            secrets = AWSClients.get_secret(env.SECRETS_DB)
            db_user = secrets.get("USERNAME")
            db_password = secrets.get("PASSWORD")
            db_host = os.getenv("DB_HOST")
            db_port = os.getenv("DB_PORT")
            db_name = os.getenv("DB_NAME")

            sql_database_url: URL = URL.create(
                'postgresql+psycopg2',
                host=db_host,
                port=db_port,
                username=db_user,
                password=db_password,
                database=db_name
            )

            self.engine = create_engine(
                sql_database_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10
            )

            self.session: Session = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine,
                expire_on_commit=False
            )(bind=self.engine)

            log.logger.info("Conexión a la base de datos establecida 🚀")
        except Exception as e:
            log.logger.error("Error al establecer la conexión a la base de datos: %s", e)
            raise

    @contextmanager
    def session_scope(self):
        """
        Maneja el ciclo de vida de la sesión
        """
        session = self.session
        try:
            yield session
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            raise
        finally:
            session.close()

    def create_all(self):
        """
        Crea las tablas en la base de datos
        """
        Base.metadata.create_all(self.engine)

    def close_session(self):
        """
        Cierra la sesión
        """
        self.session.close()
