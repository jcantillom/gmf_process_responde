from src.config.lambda_init import initialize_lambda
from src.config.config import env
from src.logs.logger import get_logger

logger = get_logger(env.DEBUG_MODE)


def lambda_handler(event, context):
    logger.info("Iniciando aplicación")
    initialize_lambda(event, context)
    logger.info("Aplicación finalizada")


# Este bloque se ejecutará solo si se está ejecutando el script localmente
if __name__ == "__main__":
    logger.info("Iniciando aplicación en entorno de desarrollo")
    # Puedes simular un evento y contexto para pruebas locales
    test_event = {}
    test_context = None
    lambda_handler(test_event, test_context)
