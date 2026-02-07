
import pandas as pd
import os

# Definição dos caminhos
ORIGEM = r"/opt/airflow/data/raw/previsao_tempo.csv"
DESTINO = r"/opt/airflow/data/processed/previsao_tempo.csv"

def extrair_dados():
    # É boa prática verificar se o arquivo de origem existe antes de ler
    if not os.path.exists(ORIGEM):
        print(f"Erro: O arquivo {ORIGEM} não foi encontrado.")
        return None
    return pd.read_csv(ORIGEM)

def salvar_em_csv(df):
    if df is None:
        return
    
    # Cria a pasta de destino (processed) se não existir
    os.makedirs(os.path.dirname(DESTINO), exist_ok=True)
    
    # Salva o arquivo
    df.to_csv(DESTINO, index=False)
    print(f"Dados processados salvos em: {DESTINO}")

if __name__ == "__main__":
    df_clima = extrair_dados()
    salvar_em_csv(df_clima)