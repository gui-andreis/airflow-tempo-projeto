
import os
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
from dotenv import load_dotenv
import pandas as pd


def extrair_dados(cidade, dias=7):
        
    data_inicio = datetime.today()
    data_fim = data_inicio + timedelta(days=dias)
    data_inicio = data_inicio.strftime('%Y-%m-%d')
    data_fim = data_fim.strftime('%Y-%m-%d')

    load_dotenv()  # Busca .env na raiz automaticamente
    VISUAL_CROSSING_KEY = os.getenv('VISUAL_CROSSING_KEY')

    city_encoded = quote(cidade)
    url = f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city_encoded}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={VISUAL_CROSSING_KEY}&contentType=csv'

    df = pd.read_csv(url)
    return df


def processar_dados(df):
    """Processa e cria colunas derivadas"""
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['temp_media'] = (df['tempmax'] + df['tempmin']) / 2
    df['amplitude_termica'] = df['tempmax'] - df['tempmin']
    df['dia_semana'] = df['datetime'].dt.day_name()
    
    colunas_importantes = ['name', 'datetime', 'tempmax', 'tempmin', 'temp', 
                          'feelslike', 'precip', 'humidity', 'windspeed', 
                          'temp_media', 'amplitude_termica', 'dia_semana']
    
    return df[colunas_importantes]
