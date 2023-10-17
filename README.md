# globant-code-challenge
Este es un entregable para el Code Challenge de Globant en el contexto de su programa de pasantías junto a Talento Digital Para Chile en el año 2023.

El desafío consiste en desarrollar un proceso de ETL que recoje datos desde una fuente, los transformar y carga los datos en BigQuery por medio de un script Python que orqueste la periodicidad del proceso (en este caso, usando Airflow).

Considerando los objetivos del challenge, la ETL se diseñó considerando el uso de tres soluciones en la nube como son Google Cloud Storage, Google Cloud Composer y BigQuery. Para este proyecto en particular aprovechamos el periodo de prueba asociada a nuestra cuenta de Google para hacer uso del ETL, pero para futuros intentos de replicar este proyecto se deberían considerar aspectos como los costos asociados a los servicios en la nube de Google.

Este documento `README.md` detalla las instrucciones para ejecutar el proceso y entender el flujo de trabajo, además de documentar los supuestos utilizados para el desarrollo del ETL con respecto a los datos.

## Propuesta de ETL
El proceso recoge datos leyendo un archivo CSV almacenado en un bucket de Cloud Storage en una tabla temporal en BigQuery, los transforma eliminando valores nulos y duplicados para segmentar los datos con una consulta SQL a la tabla temporal, y finalmente almacena el resultado de la consulta para cargarla a la tabla de destino final en BigQuery asociada al proyecto de GCP correspondiente. La orquestación para repetir de forma periódica el ETL se ejecuta por medio de un dag en una instancia de Google Cloud Composer asociada al mismo proyecto de GCP ya mencionado.

El primer archivo consiste en un script DAG que se carga en la carpeta de DAGS en la instancia de Cloud Composer, en el que se define una tarea para la extracción y transformación seguida de una tarea de carga.Para garantizar que el proceso sea compatible con la estructura de la tabla en BigQuery donde se suben los datos, el script considera procesos de validación primaria de datos, así como de remoción de valores duplicados y perdidos previo a cargar los datos en la tabla de destino. El código con el proceso se encuentra [aquí](dag.py).

El segundo archivo en este repo consiste en un script con el código que contiene los schema adecuados para las tablas de BigQuery, considerando que se hace uso de una tabla temporal para almacenar los datos desde el CSV y una tabla creada previamente como destino de los datos transformados. El archivo con los schema se encuentra [aquí](schema.py).


El dataset elegido proviene de Kaggle se llama [Netflix Movies and TV Shows](https://www.kaggle.com/datasets/shivamb/netflix-shows), el que se encuentra abierto para uso público y recoje una lista de todos los programas y películas disponibles en Netflix junto con detalles como elenco, directores, año de lanzamiento, entre otros. La data en el archivo original contiene datos de series y películas, pero utilizaremos el proceso de extracción para obtener una tabla que sólo muestre información de películas para 5 de 12 columnas.

## Requisitos e Instrucciones para desplegar ETL
Para ejecutar el proceso de ETL se necesita instalar y configurar las siguientes tecnologías:
* Cuenta GCP con acceso y configuración para las API BigQuery, Google Cloud Composer y Google Cloud Storage.
* Bucket de GCS vinculado a proyecto GCP, y Dataset y Tabla configuradas en BigQuery.
* Instancia de Google Cloud Composer vinculada al proyecto GCP correspondiente.
* Tabla de BigQuery de destino final creada con el schema correspondiente (se puede revisar en el archivo [schema](schema.py)).

Una vez listo todo lo anterior, se debe cargar el archivo DAG en la carpeta de la instancia Cloud Composer, subir el archivo en formato csv al directorio de GCS, y reemplazar estos valores en el script DAG.

## Ejecución del Proceso ETL

El proceso entrega como resultado una tabla que filtra los datos del CSV de origen para mostrar sólamente los datos de las películas disponibles en la plataforma Netflix, devolviendo las columnas con el id, título, año de lanzamiento, fecha agregada a la plataforma y duración ordenado por el año de lanzamiento en orden descendiente. La tabla puede accederse de forma pública por el enlace https://console.cloud.google.com/bigquery?project=code-challenge-401906&p=code-challenge-401906&d=netflix_movies_shows&page=dataset, pero de igual forma incluyo un screenshot con el preview de la tabla obtenido desde mi consola

![SS](https://github.com/diego-solar/globant-code-challenge/blob/main/tablafinal.png?raw=true)


