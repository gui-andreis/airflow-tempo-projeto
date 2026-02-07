import pandas as pd


tabela = pd.read_csv('/home/guiandreis/airflow-tempo-projeto/data/raw/previsao_tempo.csv')
def processar_tabela(tabela):
    
    tabela['datetime'] = pd.to_datetime(tabela['datetime'])
    tabela['temp_media'] = (tabela['tempmax'] + tabela['tempmin']) / 2
    tabela['amplitude_termica'] = tabela['tempmax'] - tabela['tempmin']
    tabela['dia_semana'] = tabela['datetime'].dt.day_name()
    
    colunas_importantes = ['name', 'datetime', 'tempmax', 'tempmin', 'temp', 'feelslike','precip','humidity','windspeed', 'temp_media', 'amplitude_termica', 'dia_semana']
    tabela_limpa = tabela[colunas_importantes]
    return tabela_limpa


