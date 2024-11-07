from src.config.lambda_init import initialize_lambda
from src.config.config import env
from src.logs.logger import get_logger


logger = get_logger(env.DEBUG_MODE)

if __name__ == "__main__":
    logger.info("Iniciando aplicación en entorno de desarrollo")
    initialize_lambda(event={}, context=None)
    logger.info("Aplicación finalizada")
