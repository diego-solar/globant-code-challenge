# Importar libs requeridas
from google.cloud import bigquery

# Definir schemas para tabla en BigQuery al que adherir DF que se cargará a BigQuery

# De acuerdo a la documentación de la API, el schema para la tabla en BigQuery debe ser la siguiente.

table_schema = {
    bigquery.SchemaField('show_id', 'STRING', mode = 'REQUIRED'),
    bigquery.SchemaField('title', 'DATETIME', mode = 'REQUIRED'),
    bigquery.SchemaField('release_year', 'INTEGER', mode = 'REQUIRED'),
    bigquery.SchemaField('date_added', 'DATE', mode = 'REQUIRED'),
    bigquery.SchemaField('duration', 'INTEGER', mode = 'REQUIRED'),
}