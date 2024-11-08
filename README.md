# Proyecto Lambda en Python para el Procesamiento de Archivos

## Descripción del Proyecto

Este proyecto es una aplicación serverless desarrollada en **Python** que se ejecuta como una **AWS Lambda**. El propósito principal es procesar archivos almacenados en **Amazon S3**, validar su estructura, descomprimirlos y registrar su información en una base de datos **PostgreSQL**. Además, el sistema envía mensajes a colas **Amazon SQS** para la integración con otros sistemas.

## Tabla de Contenidos

1. [Estructura del Proyecto](#estructura-del-proyecto)
2. [Requisitos Previos](#requisitos-previos)
3. [Instalación](#instalación)
4. [Configuración](#configuración)
5. [Uso](#uso)
6. [Despliegue](#despliegue)
7. [Pruebas](#pruebas)
8. [Mantenimiento](#mantenimiento)
9. [Créditos](#créditos)

---

## Estructura del Proyecto
```plaintext
.
├── local/
│   ├── __init__.py
│   ├── load_event.py
├── main.py
├── requirements.txt
├── README.md
├── sonar-project.properties
├── src/
│   ├── aws/
│   │   └── clients.py
│   ├── config/
│   │   ├── config.py
│   │   └── lambda_init.py
│   ├── connection/
│   │   └── database.py
│   ├── controllers/
│   │   └── archivo_controller.py
│   ├── logs/
│   │   └── logger.py
│   ├── models/
│   │   ├── base.py
│   │   ├── cgd_archivo.py
│   │   └── cgd_correo_parametro.py
│   ├── repositories/
│   │   ├── archivo_repository.py
│   │   ├── archivo_estado_repository.py
│   │   ├── catalogo_error_repository.py
│   │   └── rta_procesamiento_repository.py
│   ├── services/
│   │   ├── archivo_service.py
│   │   └── error_handling_service.py
│   └── utils/
│       ├── event_utils.py
│       ├── s3_utils.py
│       ├── sqs_utils.py
│       └── validator_utils.py
├── test_data/
│   └── event.json
└── tests/
    ├── test_archivo_service.py
    ├── test_clients.py
    ├── test_config.py
    └── test_utils.py
```


## Requisitos Previos

Antes de iniciar, asegúrate de tener instalados los siguientes programas:

- **Python 3.9** o superior.
- **pip** (gestor de paquetes de Python).
- **AWS CLI** configurado con credenciales válidas.
- **Docker** (para pruebas locales con LocalStack).
- **LocalStack** (para simular servicios de AWS).
- **PostgreSQL** (para la base de datos).

## Instalación

1. Clona este repositorio:

    ```bash
    git clone https://github.com/tu-repositorio/lambda-procesamiento.git
    cd lambda-procesamiento
    ```

2. Crea un entorno virtual:

    ```bash
    python3 -m venv venv
    source venv/bin/activate  # En Windows: venv\Scripts\activate
    ```

3. Instala las dependencias:

    ```bash
    pip install -r requirements.txt
    ```

## Configuración

### Variables de Entorno

Crea un archivo `.env` en la raíz del proyecto con el siguiente contenido:

```env
APP_ENV=local
SECRETS_DB=NGMF_RDS_POSTGRES_CREDENTIALS
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DEBUG_MODE=True

SQS_URL_PRO_RESPONSE_TO_PROCESS=http://localhost:4566/000000000000/pro-responses-to-process
SQS_URL_EMAILS=http://localhost:4566/000000000000/emails-to-send
SQS_URL_PRO_RESPONSE_TO_UPLOAD=http://localhost:4566/000000000000/pro-responses-to-upload
SQS_URL_PRO_RESPONSE_TO_CONSOLIDATE=http://localhost:4566/000000000000/pro-responses-to-consolidation

S3_BUCKET_NAME=01-bucketrtaprocesa-d01
DIR_RECEPTION_FILES=Recibidos
DIR_PROCESSED_FILES=Procesados
DIR_REJECTED_FILES=Rechazados
DIR_PROCESSING_FILES=Procesando


```
## Descripción de la Estructura del Proyecto

### `local/`
- **Archivos para pruebas locales**.
  - `load_event.py`: Carga eventos de prueba para ejecutar la Lambda localmente.

### `main.py`
- Punto de entrada principal de la Lambda.

### `requirements.txt`
- Lista de dependencias del proyecto.

### `README.md`
- Documentación del proyecto.

### `sonar-project.properties`
- Configuración para análisis de SonarQube.

### `src/`
- Código fuente principal del proyecto.

  #### `aws/`
  - **Clientes para interactuar con servicios de AWS (S3, SQS, etc)**.

  #### `config/`
  - **Configuración de la aplicación**.
    - `config.py`: Carga de variables de entorno.
    - `lambda_init.py`: Inicialización de la Lambda.

  #### `connection/`
  - **Conexión a la base de datos**.
    - `database.py`: Gestión de la conexión a PostgreSQL.

  #### `controllers/`
  - **Lógica de control para manejar las operaciones de negocio**.

  #### `logs/`
  - **Configuración de logs personalizados**.

  #### `models/`
  - **Definición de modelos y esquemas de la base de datos**.

  #### `repositories/`
  - **Capa de acceso a la base de datos**.

  #### `services/`
  - **Lógica de negocio y servicios**.

  #### `utils/`
  - **Funciones utilitarias para validaciones y manejo de S3/SQS**.

### `test_data/`
- **Datos de prueba para simular eventos**.
  - `event.json`: Ejemplo de evento para pruebas.

### `tests/`
- **Pruebas unitarias para las diferentes funcionalidades del proyecto**.
