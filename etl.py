import pandas as pd
import os
import glob

# Uma função que extrai, le e consolida o json

pasta = 'data'

def extracao_dados(pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)

    return df_total


# Um funcao de transformacao

def calcular_total_de_venda(df: pd.DataFrame) -> pd.DataFrame:
    df['Total'] = df['Quantidade'] * df['Venda']
    return df

# Uma funcao de load em csv ou parquet

def carregar_dados(df: pd.DataFrame, tipo_arquivo: list):
    for arquivo in tipo_arquivo:
        if arquivo == 'csv':
            df.to_csv("dados.csv", index=False)
        if arquivo == 'parquet':
            df.to_parquet("dados.parquet", index=False)

def pipeline_calcular_kpi_de_vendas_consolidado(pasta: str, formato_de_saida: list):
    data_frame = extracao_dados(pasta)
    data_frame_calculado = calcular_total_de_venda(data_frame)
    carregar_dados(data_frame_calculado , formato_de_saida)


