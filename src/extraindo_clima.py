import os
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote


# intervalo de datas
data_inicio = datetime.today()
data_fim = data_inicio + timedelta(days=7)

# formatando as datas
data_inicio = data_inicio.strftime('%Y-%m-%d')
data_fim = data_fim.strftime('%Y-%m-%d')

city = 'Caxias do Sul'
city_encoded = quote(city)
import os
from dotenv import load_dotenv


# Carregar o .env da pasta .gitignore
load_dotenv('/home/guiandreis/airflow-tempo-projeto/.gitignore/.env')

# Verificar se a chave foi carregada
VISUAL_CROSSING_KEY = os.getenv('VISUAL_CROSSING_KEY')

URL = (f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city_encoded}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={VISUAL_CROSSING_KEY}&contentType=csv')

df = pd.read_csv(URL)

# Salvar se quiser
path_data_raw = '/home/guiandreis/airflow-tempo-projeto/data/raw/'
df.to_csv(path_data_raw + 'previsao_tempo.csv', index=False)