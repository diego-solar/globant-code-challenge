# Importar libs requeridas
from google.cloud import bigquery

# Definir schemas para tabla en BigQuery al que adherir DF que se cargará a BigQuery

# De acuerdo a la documentación de la API, el schema para la tabla en BigQuery debe ser la siguiente.

table_schema = {
    bigquery.SchemaField('song_name', 'STRING', mode = 'REQUIRED'),
    bigquery.SchemaField('artist_name', 'STRING', mode = 'REQUIRED'),
    bigquery.SchemaField('played_at', 'DATETIME', mode = 'REQUIRED'),
    bigquery.SchemaField('timestamps', 'TIMESTAMP', mode = 'REQUIRED'),
}