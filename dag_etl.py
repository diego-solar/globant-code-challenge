# Importar libs requeridas
import datetime
import logging

# Importar DataFrame y schema para cargar en BigQuery
from apirequest import song_df
from schema import table_schema

# Importar libs de GCP y Airflow
from airflow import models
from airflow.operators import python
from google.cloud import bigquery

# Extrae Project ID desde GCP Project y configurar Dataset ID y Table ID
project_id = models.Variable.get('cloud-challenge-401906')
dataset_id = 'api_spotify'
table_ids = [
    'canciones_reproducidas',
]

# Iniciar cliente BigQuery
bigquery_client = bigquery.Client()

# Configuramos argumento DAG
default_dag_args = {
    'project_id': project_id
}

# Establece DAG junto con tareas para validar datos, segmentar usando SQL y cargar resultados a tabla BigQuery
# Utilizamos schema y funciones importadas para manejar lógica de llamada a API a Dataframe.

# Configura DAG para que tenga lugar todos los días a media noche en hora UTC
with models.DAG(
        'carga_datos',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args
) as dag:
    
# Primero se ejecuta una tarea para validar los datos, y si pasa la prueba se procede a segmentación almacenando el resultado en una variable, y luego cargando los datos segmentados a la tabla de BigQuery.
    
    # Establecemos validación de datos previo
    def validar_data() -> bool:
    
    # Check si el DF está vacío (no hay nuevos datos)
        if song_df.empty:
            print("No se descargaron canciones. Finalizando proceso.")
            return False
    # Check de llave primaria, asumiendo que la PK es la fecha en la que se reprodujo la canción
        if song_df.Series(song_df['played_at']).is_unique:
            return True
        else:
            raise Exception("Validación de Primary Key violada")
        
    task_validar = python.PythonOperator(
        task_id='task_data',
        python_callable=validar_data
    )

    #Segmentamos los datos
    def segmentar_data() -> None:
        global data_segmentada
        table_id = f"{project_id}.{dataset_id}.{table_ids[0]}"
        sql_query = """
            SELECT id, name, symbol, num_market_pairs, date_added, tags, total_supply
            FROM `your-project-id.weather_forecast.coins_marketcap`
            GROUP BY id
            ORDER BY date_added DESC
        """
        query_job = bigquery_client.query(sql_query)
        result = query_job.result()
        data_segmentada = result.to_dataframe()
        logging.info(data_segmentada)

    task_segmentar = python.PythonOperator(
        task_id='task_segmentar',
        python_callable=segmentar_data
    )
    
    def cargar_data() -> None:
        table_id = f"{project_id}.{dataset_id}.{table_ids[0]}"
        job_config = bigquery.LoadJobConfig(
            schema=table_schema
        )
        if data_segmentada is not None:
        
            job = bigquery_client.load_table_from_dataframe(
            data_segmentada, table_id, job_config=job_config
        )
        logging.info(job.result())

    task_carga = python.PythonOperator(
        task_id='task_data',
        python_callable=cargar_data
    )

# Establece dependencia de las tasks
validar_data >> segmentar_data >> cargar_data