from contextlib import contextmanager
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.session import Session
from src.aws.clients import AWSClients
from dotenv import load_dotenv
from src.config.config import env
from src.logs.logger import get_logger
from src.utils.singleton import SingletonMeta
from src.models.base import Base

# Carga las variables de entorno
load_dotenv()

logger = get_logger(env.DEBUG_MODE)


class DataAccessLayer(metaclass=SingletonMeta):
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
            db_host = env.DB_HOST
            db_port = env.DB_PORT
            db_name = env.DB_NAME

            sql_database_url = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
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
            )()

            # create tables
            Base.metadata.create_all(self.engine)

            logger.info("Conexión a la base de datos establecida")
        except Exception as e:
            logger.error("Error al establecer la conexión a la base de datos: %s", e)
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

    def close_session(self):
        """
        Cierra la sesión
        """
        self.session.close()
