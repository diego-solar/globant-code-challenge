# Importar libs requeridas
import datetime
import logging
# Importar libs de GCP y Airflow
from airflow import models
from airflow.operators import python
from data import transformar_data, cargar_data, dataset_id, table_ids
from schema import table_schema
from google.cloud import bigquery

# Definimos variables incluyendo GCP ID y variable ayer para el argumento DAG
project_id = 'code-challenge-401906'
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# Iniciar cliente BigQuery
bigquery_client = bigquery.Client()

# Configuramos argumento DAG
default_dag_args = {
    'project_id': project_id,
    'start_date': yesterday
}

# Establece DAG junto con tareas para validar datos, segmentar usando SQL y cargar resultados a tabla BigQuery
# Utilizamos schema y funciones importadas para manejar lógica de llamada a API a Dataframe.

# Configura DAG para que tenga lugar todos los días a media noche en hora UTC
with models.DAG(
        'extraer_datos','transformar_datos','cargar_datos',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args
) as dag:
        def cargar_data() -> None:
                table_id = f"{project_id}.{dataset_id}.{table_ids[0]}"
                job_config = bigquery.LoadJobConfig(
                        schema=table_schema,
                        skip_leading_rows=1,
                )
                job = bigquery_client.load_table_from_dataframe(
                transformar_data(), table_id, job_config=job_config
                )
                job.result()
                if job.state == "DONE":
    # Imprime un mensaje de éxito
                        print(f"Se cargaron {job.output_rows} filas en {table_id}")
                else:
    # Imprime un mensaje de error
                        print(f"El trabajo de carga falló con el estado {job.state}")
                        
        task_cargar = python.PythonOperator(
        task_id='task_cargar',
        python_callable=cargar_data,
        )