# Importar libs requeridas
from google.cloud import bigquery

# Definir schemas para tabla en BigQuery al que adherir DF que se cargará a BigQuery

# De acuerdo a la documentación de la API, el schema para la tabla en BigQuery debe ser la siguiente.

table_schema = {
<<<<<<< Updated upstream
    bigquery.SchemaField('song_name', 'STRING', mode = 'REQUIRED'),
    bigquery.SchemaField('artist_name', 'STRING', mode = 'REQUIRED'),
    bigquery.SchemaField('played_at', 'DATETIME', mode = 'REQUIRED'),
    bigquery.SchemaField('timestamps', 'TIMESTAMP', mode = 'REQUIRED'),
=======
    bigquery.SchemaField('show_id', 'STRING', mode = 'REQUIRED'),
    bigquery.SchemaField('title', 'STRING', mode = 'REQUIRED'),
    bigquery.SchemaField('release_year', 'INTEGER', mode = 'REQUIRED'),
    bigquery.SchemaField('date_added', 'DATE', mode = 'REQUIRED'),
    bigquery.SchemaField('duration', 'STRING', mode = 'REQUIRED'),
}

temp_table_schema = {
    bigquery.SchemaField('show_id', 'STRING', mode = 'REQUIRED'),
    bigquery.SchemaField('type', 'STRING', mode = 'REQUIRED'),
    bigquery.SchemaField('title', 'STRING', mode = 'REQUIRED'),
    bigquery.SchemaField('director', 'STRING', mode = 'NULLABLE'),
    bigquery.SchemaField('cast', 'STRING', mode = 'NULLABLE'),
    bigquery.SchemaField('country', 'STRING', mode = 'NULLABLE'),
    bigquery.SchemaField('date_added', 'DATE', mode = 'REQUIRED'),
    bigquery.SchemaField('release_year', 'INTEGER', mode = 'REQUIRED'),
    bigquery.SchemaField('rating', 'STRING', mode = 'NULLABLE'),
    bigquery.SchemaField('duration', 'STRING', mode = 'REQUIRED'),    
    bigquery.SchemaField('listed_in', 'STRING', mode = 'NULLABLE'),
    bigquery.SchemaField('description', 'STRING', mode = 'NULLABLE'),
>>>>>>> Stashed changes
}