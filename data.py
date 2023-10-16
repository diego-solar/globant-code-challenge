# Importamos libs requeridas y schema desde el archivo schema

import csv
import pandas as pd
import datetime
import urllib.request
from io import StringIO
from google.cloud import bigquery
from schema import table_schema

# Definimos variables incluyendo cliente BigQuery, URI del archivo csv y el ID´s de GCP/GCS
client = bigquery.Client()
source_uri = 'gs://southamerica-east1-code-cha-e2941630-bucket/netflix_titles.csv'
table_id = 'code-challenge-401906.netflix_movies_shows.netflix_movies'
project_id = 'code-challenge-401906'
dataset_id = 'netflix_movies_shows'
table_ids = 'netflix_movies'

# Creamos una tabla temporal de BigQuery a partir de la lista de filas estableciendo expiración en una hora
temp_table_id = f"{client.project}.{dataset_id}.temp_table"
table = bigquery.Table(table_id, schema=table_schema)
table.expires = datetime.datetime.now() + datetime.timedelta(hours=1)
table = client.create_table(table, exists_ok=True)

# El ETL primero ejecuta una tarea de validación de datos, luego una tarea de segmentación y limpieza, y al final una tarea que carga a la tabla en BigQuery.
# Los datos se extraen a la tabla temporal en BigQuery, y con ella ejecutamos la segmentación con SQL previa a la carga final en BigQuery.
# Definimos las funciones para validar, segmentar y cargar los datos.

# Función que verifica que el csv no esté vacío, verifica que la columna de identificador primario no está duplicado (Validación de Primary Key) y extrae las filas a un df
def extraer_data(source_uri) -> bool:
    
    response = urllib.request.urlopen(source_uri)
    data = response.read().decode('utf-8')
    csvfile = StringIO(data)
    reader = csv.reader(csvfile)
    next(reader)
    
    if len(csvfile.getvalue()) == 0:
        print("El archivo está vacío. Finalizando proceso.")
        return False
    rows = []
    reader = csv.reader(csvfile)
    while True:
        try:
            row = next(reader)
            rows.append(row)
        except StopIteration:
            break
    # Convertir la lista de filas en un dataframe de pandas
    df = pd.DataFrame(rows, columns=['show_id'])
    # Loop las filas del archivo para verificar que la columna de Primary Key no está duplicada
    if not df['show_id'].is_unique:
        raise Exception("Validación de Primary Key violada: hay valores duplicados en la columna 'show_id'")
        # Carga los datos de la lista de filas en la tabla temporal
    errors = client.insert_rows(table, rows, skip_invalid_rows=True)

    if errors:
            print(f"Se encontraron errores al insertar las filas: {errors}")
    else:
            print(f"Se insertaron {len(rows)} filas en la tabla temporal")

# Función que limpia entradas duplicadas y nulls, y luego segmenta los datos de la tabla temporal con través de un query SQL
def transformar_data(temp_table_id) -> pd.DataFrame:
    global data_segmentada
    # Define una consulta SQL que usa la tabla temporal
    query = f"SELECT show_id, title, release_year, date_added, duration FROM `{temp_table_id}` GROUP BY release_year ORDER BY date_added DESC"
    # Ejecuta la consulta y obtiene los resultados
    query_job = client.query(query)
    resultado = query_job.result()
    # Crea un DataFrame de pandas a partir de los resultados
    data_segmentada = resultado.to_dataframe()
    # Retorna el DataFrame como el valor de la función
    return data_segmentada