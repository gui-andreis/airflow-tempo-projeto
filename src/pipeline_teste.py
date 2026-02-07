# pipeline_tempo.py
import os
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values


def extrair_dados(cidade, dias=7):
    data_inicio = datetime.today()
    data_fim = data_inicio + timedelta(days=dias)

    data_inicio = data_inicio.strftime('%Y-%m-%d')
    data_fim = data_fim.strftime('%Y-%m-%d')

    load_dotenv()
    VISUAL_CROSSING_KEY = os.getenv('VISUAL_CROSSING_KEY')

    city_encoded = quote(cidade)

    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{city_encoded}/{data_inicio}/{data_fim}"
        f"?unitGroup=metric&include=days&key={VISUAL_CROSSING_KEY}&contentType=csv"
    )

    df = pd.read_csv(url)
    return df


def processar_dados(df):
    """Processa e cria colunas derivadas"""
    df['datetime'] = pd.to_datetime(df['datetime'])

    df['avg_temp'] = (df['tempmax'] + df['tempmin']) / 2
    df['temp_range'] = df['tempmax'] - df['tempmin']
    df['day_of_week'] = df['datetime'].dt.day_name()

    important_columns = [
        'name', 'datetime', 'tempmax', 'tempmin', 'temp',
        'feelslike', 'precip', 'humidity', 'windspeed',
        'avg_temp', 'temp_range', 'day_of_week'
    ]

    return df[important_columns]

def carregar_postgres(df, tabela="previsao_tempo"):
    """
    Envia os dados processados para o Postgres.
    Usa INSERT com UPSERT pra não duplicar registros.
    """

    load_dotenv()

    DB_HOST =  "postgres"
    DB_NAME =  "clima_db"
    DB_USER =  "airflow"
    DB_PASSWORD ="airflow"
    DB_PORT =  "5432"

    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

    cursor = conn.cursor()

    # cria tabela se não existir
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {tabela} (
        name TEXT NOT NULL,
        datetime TIMESTAMP NOT NULL,

        tempmax DOUBLE PRECISION,
        tempmin DOUBLE PRECISION,
        temp DOUBLE PRECISION,
        feelslike DOUBLE PRECISION,
        precip DOUBLE PRECISION,
        humidity DOUBLE PRECISION,
        windspeed DOUBLE PRECISION,

        avg_temp DOUBLE PRECISION,
        temp_range DOUBLE PRECISION,
        day_of_week TEXT,

        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (name, datetime)
        );
    """)

    conn.commit()

    # transforma df em lista de tuplas
    registros = [
        (
            row["name"],
            row["datetime"],
            row["tempmax"],
            row["tempmin"],
            row["temp"],
            row["feelslike"],
            row["precip"],
            row["humidity"],
            row["windspeed"],
            row["avg_temp"],
            row["temp_range"],
            row["day_of_week"]
        )
        for _, row in df.iterrows()
    ]

    query = f"""
        INSERT INTO {tabela} (
            name, datetime, tempmax, tempmin, temp,
            feelslike, precip, humidity, windspeed,
            avg_temp, temp_range, day_of_week
        )
        VALUES %s
        ON CONFLICT (name, datetime)
        DO UPDATE SET
            tempmax = EXCLUDED.tempmax,
            tempmin = EXCLUDED.tempmin,
            temp = EXCLUDED.temp,
            feelslike = EXCLUDED.feelslike,
            precip = EXCLUDED.precip,
            humidity = EXCLUDED.humidity,
            windspeed = EXCLUDED.windspeed,
            avg_temp = EXCLUDED.avg_temp,
            temp_range = EXCLUDED.temp_range,
            day_of_week = EXCLUDED.day_of_week,
            collected_at = CURRENT_TIMESTAMP;
    """

    execute_values(cursor, query, registros)
    conn.commit()

    cursor.close()
    conn.close()

    print(f"✅ Dados inseridos/atualizados no Postgres na tabela: {tabela}")
