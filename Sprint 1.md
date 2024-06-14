## Sprint 1: Extracción de datos en tiempo real utilizando APIs y procesamiento inicial

## Primero se importa las bibliotecas necesarias
import pandas as pd
import json
import requests

# URL de la API de WeatherAPI
url = "http://api.weatherapi.com/v1/current.json"
params = {
    'key': '9e8bd3955fa648be86f210808241006',
 # lugar donde revisar el clima
    'q': 'lima',
# calidad de aire
    'aqi': 'yes' 
}

# Realizar la solicitud GET
response = requests.get(url, params=params)

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
# Convertir la respuesta a formato JSON
    json_data = response.json()
# Imprimir la respuesta JSON
    print(json.dumps(json_data,indent=4))

# Extraer los datos de 'location' y 'current' y combinarlos
    data = {**json_data['location'], **json_data['current'], **json_data['current']['condition'], **json_data['current']['air_quality']}
# Eliminar claves innecesarias que han sido aplanadas
    data.pop('condition', None)
    data.pop('air_quality', None)

# Crear el DataFrame
    df = pd.DataFrame([data])

# Mostrar el DataFrame
    print(df)

else:
# Imprimir el código de estado de la respuesta en caso de error
    print(f"Error: {response.status_code}")


df
