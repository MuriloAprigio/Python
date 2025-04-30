import pyodbc
import pandas as pd
import numpy as np
import datetime
from concurrent.futures import ThreadPoolExecutor

# Connection string
connection_string = (
    'SUA CONEXÃO NO BANCO DE DADOS};'
    'SUA CONEXÃO NO BANCO DE DADOS;'
    'SUA CONEXÃO NO BANCO DE DADOS;'
    'SUA CONEXÃO NO BANCO DE DADOS;'
    'SUA CONEXÃO NO BANCO DE DADOS;'
    'Encrypt=yes;TrustServerCertificate=no;'
    'Connection Timeout=30;'
    'Authentication=ActiveDirectoryPassword'
)

# Função para tentar converter para date
def try_parse_date(text):
    if isinstance(text, str):
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'):
            try:
                return datetime.datetime.strptime(text, fmt).date()
            except ValueError:
                pass
    elif isinstance(text, datetime.datetime):
        return text.date()
    return text

# Função para processar os dados em um chunk
def process_data_chunk(df_chunk):
    try:
        connection = pyodbc.connect(connection_string)
        connection.autocommit = False
        cursor = connection.cursor()

        sql_insert_query = """
            INSERT INTO [CATALOGO_EXEMPLO].[TBL_SCHEMA_NOME_TABELA] (COLUNA_1, COLUNA_2, COLUNA_3) 
            VALUES (?, ?, ?)
        """

        # Garantir que os dados sejam strings e substituir NaNs por None
        df_chunk = df_chunk.where(pd.notnull(df_chunk), None)

        # Converter os dados da coluna "DESEJADA"
        df_chunk['COLUNA_1'] = df_chunk['COLUNA_1'].apply(lambda x: try_parse_date(x) if x is not None else None)

        data_to_insert = df_chunk[['COLUNA_1', 'COLUNA_2', 'COLUNA_3']].values.tolist()
        cursor.executemany(sql_insert_query, data_to_insert)

        connection.commit()
        cursor.close()
        connection.close()
    except pyodbc.Error as e:
        print(f"Erro ao inserir o lote: {e}")
        if connection:
            connection.rollback()
        return df_chunk
    return None

# Função para processar os dados em lotes em paralelo
def chunk_insert_data_async(df, chunk_size=100, max_workers=1):
    data_chunks = np.array_split(df, len(df) // chunk_size)
    failed_chunks = []
    print(f"Início processamento dos dados: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_data_chunk, chunk): chunk for chunk in data_chunks}
        for future in futures:
            result = future.result()
            if result is not None:
                failed_chunks.append(result)
    print(f"Processo de inserção concluído: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Fim processamento dos dados: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return failed_chunks

# Caminho para o arquivo CSV
csv_file_path = r'C:\Users\murilo.ana\Downloads\Arquivo_exemplo.csv'

print("Início Consulta dados API: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Ler o arquivo CSV e tratar valores nulos
df = pd.read_csv(csv_file_path, dtype=str, na_values=['NULL', 'nan', 'NaN', ''])

# Substituir NaNs por None
df = df.where(pd.notnull(df), None)

# Chamar a função para processar os dados em paralelo
failed_chunks = chunk_insert_data_async(df, chunk_size=100, max_workers=1)

# Tentativa de reprocessar os chunks falhados
retry_failed_chunks = []
if failed_chunks:
    print("Reprocessando chunks falhados...")
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = {executor.submit(process_data_chunk, chunk): chunk for chunk in failed_chunks}
        for future in futures:
            result = future.result()
            if result is not None:
                retry_failed_chunks.append(result)

# Salvar linhas que falharam após a tentativa de reprocessamento
if retry_failed_chunks:
    print("Salvando linhas que falharam após tentativas de inserção...")
    failed_df = pd.concat(retry_failed_chunks)
    failed_df.to_csv(r'C:\Users\murilo.ana\Downloads\Linhas_Falhadas.csv', index=False)
    print("Linhas falhadas salvas em 'Linhas_Falhadas.csv'.")
else:
    print("Nenhuma linha falhou após as tentativas de reprocessamento.")
