from src.config.lambda_init import initialize_lambda
from src.logs import logger as log

if __name__ == "__main__":
    log.logger.info("Iniciando aplicación en entorno de desarrollo")
    initialize_lambda(event={}, context=None)
    log.logger.info("Aplicación finalizada")