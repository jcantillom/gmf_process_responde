from src.config.lambda_init import initialize_lambda
from src.config.config import env  # Importa env para acceder a la configuración de entorno
from src.logs.logger import get_logger  # Importa el logger configurado

# Inicializa el logger basado en el valor de DEBUG_MODE de la configuración
logger = get_logger(env.DEBUG_MODE)

if __name__ == "__main__":
    logger.info("Iniciando aplicación en entorno de desarrollo")
    initialize_lambda(event={}, context=None)
    logger.info("Aplicación finalizada")
