# importamos libs requeridas

import requests
import json
import pandas as pd
from datetime import datetime
import datetime

# Definimos constantes de acuerdo a instrucciones del API

user_id = ''
token = ''

# Establecemos validación de datos previo al proceso ETL

def validacion_datos(df: pd.DataFrame) -> bool:
    
    # Check si el DF está vacío (no hay nuevos datos)
    if df.empty:
        print("No se descargaron canciones. Finalizando proceso.")
        return False
    # Check llave primaria, asumiendo que la PK es la fecha en la que se reprodujo la canción
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Validación de Primary Key violada")
    return True

# Proceso de extracción de datos usando API
# La API requiere incluir campos con fecha en unix para realizar la solicitud

if __name__ == '__main__':
    headers = {
        'Accepts' : 'application/json',
        'Content-Type': 'application/json',
        'Authorization' : 'Bearer {token}'.format(token=token)
    }
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix_timestamp, headers = headers))
    data = r.json

    song_names =[]
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data['items']:
            song_names.append(song['track']['name'])
            artist_names.append(song['track']['album']['artists'[0]]['name'])
            played_at_list.append(song['played_at'])
            timestamps.append(song['played_at'][0:10])
    
    song_dict = {
        'song_name' : song_names,
        'artist_name' : artist_names,
        'played_at' : played_at_list,
        'timestamps' : timestamps
    }

# Crea DF que almacena datos extraídos

    song_df = pd.DataFrame(song_dict, columns= ['song_name', 'artist_name', 'played_at', 'timestamp'])

# Realizamos limpieza y transformación de datos eliminando duplicados y valores faltantes
song_df.dropna(inplace=True)
song_df.drop_duplicates(inplace=True)

print(song_df)

if validacion_datos(song_df):
        print("Validación de datos aprobada, continuar con proceso ETL.")
else:
        print("Error en la validación de datos, se detendrá el proceso ETL.")