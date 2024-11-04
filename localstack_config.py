import boto3
import json
import os

# Configuración para LocalStack
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
endpoint_url = 'http://localhost:4566'

# 1. Crear el cliente para Secrets Manager
secrets_manager_client = boto3.client('secretsmanager', endpoint_url=endpoint_url)

# Crear secreto en Secrets Manager
secret_name = "NGMF_RDS_POSTGRES_CREDENTIALS"
secret_value = json.dumps({"USERNAME": "postgres", "PASSWORD": "postgres"})
try:
    secrets_manager_client.create_secret(Name=secret_name, SecretString=secret_value)
    print(f"Secreto '{secret_name}' creado exitosamente en Secrets Manager.")
except secrets_manager_client.exceptions.ResourceExistsException:
    print(f"El secreto '{secret_name}' ya existe en Secrets Manager.")

# 2. Crear el cliente para Systems Manager (SSM)
ssm_client = boto3.client('ssm', endpoint_url=endpoint_url)

# Crear parámetros en Parameter Store para configuración de reintentos
retries_parameters = [
    {
        "Name": "/gmf/transversal/config-retries",
        "Value": json.dumps({"number-retries": "5", "time-between-retry": "900"})
    },
    {
        "Name": "/gmf/process-responses/config-retries",
        "Value": json.dumps({"time-between-retry": 120})
    },
    {
        "Name": "/ngmf/transversal/config-retries",
        "Value": json.dumps({"number-retries": "5", "time-between-retry": "900"})
    },
    {
        "Name": "/ngmf/process-responses/config-retries",
        "Value": json.dumps({"time-between-retry": 120})
    }
]

# Crear los parámetros en Parameter Store
for param in retries_parameters:
    ssm_client.put_parameter(Name=param["Name"], Value=param["Value"], Type='String', Overwrite=True)
    print(f"Parámetro '{param['Name']}' creado exitosamente en Parameter Store.")

# Crear parámetro específico para archivos especiales
special_files_param_name = "/gmf/process-responses/general-config"
special_files_param_value = json.dumps({
    "start-special-files": "RE_ESP_TUTGMF00010039",
    "end-special-files": "-0001",
    "start-name-files-rta": "RE_PRO_TUTGMF00010039",
    "valid_states_process": [
        "ENVIADO",
        "PREVALIDADO",
        "PROCESAMIENTO_FALLIDO",
        "PROCESA_PENDIENTE_REINTENTO",
        "PROCESAMIENTO_RECHAZADO"
    ]
})
ssm_client.put_parameter(Name=special_files_param_name, Value=special_files_param_value, Type='String', Overwrite=True)
print(f"Parámetro '{special_files_param_name}' creado exitosamente en Parameter Store.")

# 3. Crear el cliente para S3
s3_client = boto3.client('s3', endpoint_url=endpoint_url)

# Crear el bucket en S3 y añadir carpetas
bucket_name = "01-bucketrtaprocesa-d01"
try:
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' creado exitosamente en S3.")
except s3_client.exceptions.BucketAlreadyOwnedByYou:
    print(f"El bucket '{bucket_name}' ya existe en S3.")

# Crear carpetas en el bucket
folders = ["Recibidos/", "Procesando/", "Rechazados/", "Procesados/"]
for folder in folders:
    s3_client.put_object(Bucket=bucket_name, Key=f"{folder}.keep", Body="")
    print(f"Carpeta '{folder}' creada exitosamente en el bucket '{bucket_name}'.")

# 4. Crear el cliente para SQS
sqs_client = boto3.client('sqs', endpoint_url=endpoint_url)

# Crear colas en SQS
queues = ["pro-responses-to-process", "pro-responses-to-validate", "pro-responses-to-consolidate", "emails-to-send",
          "pro-responses-to-reception"]
for queue in queues:
    sqs_client.create_queue(QueueName=queue)
    print(f"Cola '{queue}' creada exitosamente en SQS.")

# Mensaje final
print("Todos los recursos de AWS han sido creados exitosamente en LocalStack.")
