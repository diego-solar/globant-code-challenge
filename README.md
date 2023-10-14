# globant-code-challenge
Este es un entregable para el Code Challenge de Globant en el contexto de su programa de pasantías junto a Talento Digital Para Chile en el año 2023.

El desafío consiste en desarrollar un proceso de ETL que recoje datos desde una fuente, los transformar y carga los datos en BigQuery por medio de un script Python que orqueste la periodicidad del proceso (en este caso, usando Airflow).

Considerando los objetivos del challenge, la ETL se diseñó considerando el uso de tres soluciones en la nube como son Google Cloud Storage, Google Cloud Composer y BigQuery. Para este proyecto en particular aprovechamos el periodo de prueba asociada a nuestra cuenta de Google para hacer uso del ETL, pero para futuros intentos de replicar este proyecto se deberían considerar aspectos como los costos asociados a los servicios en la nube de Google.

Este documento `README.md` detalla las instrucciones para ejecutar el proceso y entender el flujo de trabajo, además de documentar los supuestos utilizados para el desarrollo del ETL con respecto a los datos.

## Propuesta de ETL
El proceso recoge datos por medio de la API de Spotify, los transforma y finalmente los carga a una tabla de BigQuery alojada en un proyecto de GCP, mientras que se orquesta la repetición periódica de tareas usando Google Cloud Composer asociada al mismo proyecto de GCP ya mencionado.

La data proviene de la API oficial de Spotify con las últimas canciones reproducidas por cada usuario. El código que administra la extracción de los datos de la api se encuentra en el archivo [DAG (dag_etl.py)].

Para garantizar que el proceso sea compatible con la estructura de la tabla en BigQuery donde se suben los datos, se incluye el código que contiene el schema adecuado para los datos extraídos y se encuentra detallado en [Schema (schema.py)].

El último componente del pipeline es el [DAG (dag_etl.py)], el que establece las tareas para segmentar y cargar los datos, previo a una tarea de Validación de Datos que asegura la integridad de los mismos.

## Requisitos e Instrucciones para desplegar ETL
Para ejecutar el proceso de ETL se necesita instalar y configurar las siguientes tecnologías:
* Cuenta GCP con acceso y configuración para las API BigQuery, Google Cloud Composer y Google Cloud Storage.
* Bucket de GCS vinculado a proyecto GCP, y Dataset y Tabla configuradas en BigQuery.
* Instancia de Google Cloud Composer vinculada al proyecto GCP correspondiente.
* Token de acceso para la API de Spotify que puede pedirse (aquí [https://developer.spotify.com/console/get-recently-played/])

Una vez listo todo lo anterior, se debe reemplazar el token de la API, el nombre del proyecto, dataset y tabla de destino en el archivo (apirequest.py), y luego subir los tres archivos .py en el bucket de GCP asociado a la instancia de Google Composer.

## Ejecución del Proceso ETL

## Supuestos y Consideraciones de los datos