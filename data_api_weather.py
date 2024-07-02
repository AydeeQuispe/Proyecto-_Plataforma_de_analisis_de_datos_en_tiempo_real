import dask.dataframe as dd
import streamlit as st
import pandas as pd
import json 
import requests

st.title("Geolocalización y clima de la ciudad de Lima")
# URL de la API de WeatherAPI
url = "http://api.weatherapi.com/v1/current.json"
params = {
    'key': '9e8bd3955fa648be86f210808241006',
    'q': 'lima', # lugar donde revisar el clima
    'aqi': 'yes' # calidad de aire
}

# Realizar la solicitud GET
response = requests.get(url, params=params)

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
    # Convertir la respuesta a formato JSON
    
    json_data = response.json()
    
    # Imprimir la respuesta JSON
    # print(json.dumps(data,indent=4))

    # Extraer los datos de 'location' y 'current' y combinarlos
    data = {**json_data['location'], **json_data['current'], **json_data['current']['condition'], **json_data['current']['air_quality']}
    # Eliminar claves innecesarias que han sido aplanadas
    data.pop('condition', None)
    data.pop('air_quality', None)

    # Crear el DataFrame
    df = pd.DataFrame([data])

    # Definir el archivo CSV donde se almacenarán los datos
    archivo_csv = 'historico_datos.csv'

    # Leer el archivo CSV existente o crear uno nuevo si no existe
    try:
        df_historico = dd.read_csv(archivo_csv)
    except FileNotFoundError:
        df_historico = dd.from_pandas(pd.DataFrame(), npartitions=1)

    # Convertir el DataFrame actual en un Dask DataFrame
    df_dask = dd.from_pandas(df, npartitions=1)

    # Apilar los nuevos datos al DataFrame histórico
    df_actualizado = dd.concat([df_historico, df_dask], axis=0)

    # Guardar el DataFrame actualizado en el archivo CSV
    df_actualizado.to_csv(archivo_csv, single_file=True, index=False)

    # Leer el archivo CSV actualizado para mostrarlo en Streamlit
    df_mostrar = dd.read_csv(archivo_csv).compute()


    st.header('Tabla del  Historial de datos')
    st.dataframe(df_mostrar)

    # Mostrar el DataFrame
    # print(df)

    st.header('Json en tiempo real de geolocalización y clima de la ciudad de Lima')
    st.json(json_data)
else:
    # Imprimir el código de estado de la respuesta en caso de error
    print(f"Error: {response.status_code}")
