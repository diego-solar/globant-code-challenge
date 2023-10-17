# Importar libs requeridas
import datetime
import csv
import pandas as pd
import urllib.request
from io import StringIO
from schema import table_schema, temp_table_schema

# Importar libs de GCP y Airflow
from airflow import models
from airflow.operators import python
from google.cloud import bigquery


# Definimos variables incluyendo GCP ID y variable ayer para el argumento DAG
project_id = 'code-challenge-401906'
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())
source_uri = 'gs://southamerica-east1-code-cha-e2941630-bucket/netflix_titles.csv'
public_url = 'https://storage.googleapis.com/southamerica-east1-code-cha-e2941630-bucket/netflix_titles.csv'
table_id = 'code-challenge-401906.netflix_movies_shows.netflix_movies'
project_id = 'code-challenge-401906'
dataset_id = 'netflix_movies_shows'
table_ids = 'netflix_movies'

# Iniciar cliente BigQuery
client = bigquery.Client()

# Creamos una tabla temporal de BigQuery a partir de la lista de filas estableciendo expiración en una hora
temp_table_id = f"{client.project}.{dataset_id}.temp_table"
table = bigquery.Table(temp_table_id, schema=temp_table_schema)
table.expires = datetime.datetime.now() + datetime.timedelta(hours=1)
table = client.create_table(table, exists_ok=True)

# Configuramos argumento DAG
default_dag_args = {
    'project_id': project_id,
    'start_date': yesterday,
    'public_url': public_url,
    'temp_table_id': temp_table_id,
    'table_id' : table_id
}

# Establece DAG junto con tareas para validar datos, segmentar usando SQL y cargar resultados a tabla BigQuery
# Configura DAG para que tenga lugar todos los días a media noche en hora UTC
def extraer_transformar(public_url, temp_table_id) -> bool:
                global data_segmentada
                try:
                        response = urllib.request.urlopen(public_url)
                        data = response.read().decode('utf-8')
                        csvfile = StringIO(data)
                        reader = csv.reader(csvfile)
                        next(reader)

                        if len(csvfile.getvalue()) == 0:
                                print("El archivo está vacío. Finalizando proceso.")
                                return False
                
                        rows = []
                        while True:
                                try:
                                        row = next(reader)
                                        rows.append(row)
                                except StopIteration:
                                        break

                        # Convierte lista de filas a un df, busca valores duplicados para regla de Primary Key e inserta valores en la tabla temporal
                        df = pd.DataFrame(rows, columns=['show_id', 'type', 'title', 'director', 'cast', 'country','date_added', 'release_year','rating' ,'duration', 'listed_in', 'description'])
                        df['show_id'] = df['show_id'].astype(str)
                        df['type'] = df['type'].astype(str)
                        df['title'] = df['title'].astype(str)
                        df['director'] = df['director'].astype(str)
                        df['cast'] = df['cast'].astype(str)
                        df['country'] = df['country'].astype(str)
                        df['date_added'] = pd.to_datetime(df['date_added']).dt.date
                        df['release_year'] = df['release_year'].astype(int)
                        df['rating'] = df['rating'].astype(str)
                        df['duration'] = df['duration'].astype(str)
                        df['listed_in'] = df['listed_in'].astype(str)
                        df['description'] = df['description'].astype(str)

                        if not df['show_id'].is_unique:
                                raise Exception("Validación de Primary Key violada: hay valores duplicados en la columna 'show_id'")

                        job_config = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.PARQUET,
                        schema=temp_table_schema,
                        max_bad_records = 20,
                        )
                        carga_temp = client.load_table_from_dataframe(
                        df, temp_table_id, job_config=job_config
                        )
                        carga_temp.result()
                        if carga_temp.errors:
                                print(f"Se encontraron errores al insertar las filas: {carga_temp}")
                        else:
                                print(f"Se insertaron {carga_temp.output_rows} filas en la tabla temporal")
                        
                except Exception as e:
                        print(f"Ocurrió un error en: {str(e)}")
                        return None


def cargar_data() -> None:
# Define un query SQL para segmentar los datos extraídos, los almacena a un df como variable removiendo duplicados y nulls
        query = f"SELECT show_id, title, release_year, date_added, REGEXP_REPLACE(duration, '[^0-9]', '') AS duration FROM `{temp_table_id}` WHERE type <> 'TV Show' ORDER BY release_year DESC"
        query_job = client.query(query)
        resultado = query_job.result()
        data_segmentada = resultado.to_dataframe()
        data_segmentada.dropna(inplace=True)
        data_segmentada.drop_duplicates(inplace=True)

# Verificar si el dataframe data_segmentada es None
        if data_segmentada is None:
# Imprimir un mensaje de error y terminar la función
                print("El dataframe data_segmentada es None. No se puede cargar a la tabla de destino.")
                return
# Continuar con el proceso de carga
        job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=table_schema,

)
        job = client.load_table_from_dataframe(
                data_segmentada, table_id, job_config=job_config
                )

        job.result()
        if job.state == "DONE":
    # Imprime un mensaje de éxito
                        print(f"Se cargaron {job.output_rows} filas en {table_id}")
        else:
    # Imprime un mensaje de error
                        print(f"El trabajo de carga falló con el estado {job.state}")
with models.DAG(
        'etl',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args
) as dag:
        task_data = python.PythonOperator(
        task_id='task_data',
        python_callable=extraer_transformar,
        op_kwargs={'public_url': public_url, 'temp_table_id': temp_table_id},
        dag=dag
        )

        task_cargar = python.PythonOperator(
        task_id='task_cargar',
        python_callable=cargar_data,
        dag = dag
        )
# Establecemos dependencia para ejecutar las tareas
task_data >> task_cargar