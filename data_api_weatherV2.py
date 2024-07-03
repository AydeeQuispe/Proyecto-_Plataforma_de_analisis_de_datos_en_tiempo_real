import dask.dataframe as dd
import streamlit as st
import pandas as pd
import json
import requests
import time
from prometheus_client import Summary, Gauge, generate_latest, CollectorRegistry
import threading

# Crear un registro personalizado
registry = CollectorRegistry()

# Crear métricas de Prometheus
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request', registry=registry)
DATAFRAME_UPDATE_TIME = Summary('dataframe_update_seconds', 'Time spent updating dataframe', registry=registry)
REQUEST_SUCCESS = Gauge('request_success', 'Indicates if the request was successful (1) or not (0)', registry=registry)

# Iniciar el servidor de Prometheus en un hilo separado
def start_prometheus_server():
    from prometheus_client import start_http_server
    start_http_server(8000)

threading.Thread(target=start_prometheus_server).start()

st.title("Geolocalización y clima de la ciudad de Lima")

# URL de la API de WeatherAPI
url = "http://api.weatherapi.com/v1/current.json"
params = {
    'key': '9e8bd3955fa648be86f210808241006',
    'q': 'lima',  # lugar donde revisar el clima
    'aqi': 'yes'  # calidad de aire
}

# Medir el tiempo de la solicitud
start_time = time.time()
response = requests.get(url, params=params)
elapsed_time = time.time() - start_time
REQUEST_TIME.observe(elapsed_time)

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
    REQUEST_SUCCESS.set(1)
    # Convertir la respuesta a formato JSON
    json_data = response.json()

    # Extraer los datos de 'location' y 'current' y combinarlos
    data = {**json_data['location'], **json_data['current'], **json_data['current']['condition'], **json_data['current']['air_quality']}
    # Eliminar claves innecesarias que han sido aplanadas
    data.pop('condition', None)
    data.pop('air_quality', None)

    # Crear el DataFrame
    df = pd.DataFrame([data])

    # Medir el tiempo de actualización del DataFrame
    start_time = time.time()
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

    elapsed_time = time.time() - start_time
    DATAFRAME_UPDATE_TIME.observe(elapsed_time)

    # Leer el archivo CSV actualizado para mostrarlo en Streamlit
    df_mostrar = dd.read_csv(archivo_csv).compute()

    st.header('Tabla del Historial de datos')
    st.dataframe(df_mostrar)

    st.header('Json en tiempo real de geolocalización y clima de la ciudad de Lima')
    st.json(json_data)
else:
    REQUEST_SUCCESS.set(0)
    st.error(f"Error: {response.status_code}")

# Extraer y mostrar métricas de Prometheus
st.header("Métricas de Prometheus")
metrics_data = generate_latest(registry).decode('utf-8')
st.text(metrics_data)
