import os
import datetime
import requests
import zipfile
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from io import BytesIO


aws_access_key_id=''
aws_secret_access_key=''
aws_session_token=''
bucket_name = 'tech-fiap-fase2-s3'
s3_path = 'raw-b3/scrap-dados/'

# Função para encontrar o último dia útil
def ultimo_dia_util(data):
    while data.weekday() >= 5:  # 5 = sábado, 6 = domingo
        data -= datetime.timedelta(days=1)
    return data

# Data atual
data_atual = datetime.date.today()

# Data de ontem
data_ontem = data_atual - datetime.timedelta(days=1)

# Verificar se a data de ontem é um dia útil
if data_ontem.weekday() < 5:  # 0 = segunda-feira, 1 = terça-feira, ..., 4 = sexta-feira
    data_ontem = data_ontem
else:
    # Encontrar o último dia útil
    data_ontem = ultimo_dia_util(data_ontem)

# URL do arquivo .zip
url = f'https://arquivos.b3.com.br/rapinegocios/tickercsv/{data_ontem}'

# Fazer a requisição HTTP para baixar o arquivo .zip
print("Baixando o arquivo .zip...")
response = requests.get(url)
zip_content = BytesIO(response.content)
print("Arquivo .zip baixado com sucesso.")

# Extrair o arquivo .txt do .zip
with zipfile.ZipFile(zip_content, 'r') as zip_ref:
    
    txt_filename = zip_ref.namelist()[0]
    print(f"Extraindo o arquivo {txt_filename} do .zip...")
    with zip_ref.open(txt_filename) as txt_file:
        # Carregar os dados do arquivo .txt em chunks
        chunk_size = 100000  # Número de linhas por chunk
        df_chunks = pd.read_csv(txt_file, delimiter=';', encoding='utf-8', chunksize=chunk_size)
        print("Arquivo .txt carregado em chunks.")
        
        # Configurar o sistema de arquivos S3 com credenciais
        s3_fs = s3fs.S3FileSystem(
            key=aws_access_key_id,
            secret=aws_secret_access_key,
            token=aws_session_token
        )
        
        # Processar cada chunk
        for i, pregao in enumerate(df_chunks):
            # Tratar os tipos de dados das colunas
            pregao['DataReferencia'] = pd.to_datetime(pregao['DataReferencia']).astype('datetime64[ms]')

            pregao['CodigoInstrumento'] = pregao['CodigoInstrumento'].astype(str)
            pregao['AcaoAtualizacao'] = pregao['AcaoAtualizacao'].fillna(0).astype(int)

            # Converter para string, remover vírgulas e converter para float
            pregao['PrecoNegocio'] = pregao['PrecoNegocio'].astype(str).str.replace(',', '').astype(float)

            pregao['QuantidadeNegociada'] = pregao['QuantidadeNegociada'].fillna(0).astype(int)
            pregao['HoraFechamento'] = pregao['HoraFechamento'].fillna(0).astype(int)
            pregao['CodigoIdentificadorNegocio'] = pregao['CodigoIdentificadorNegocio'].fillna(0).astype(int)
            pregao['TipoSessaoPregao'] = pregao['TipoSessaoPregao'].fillna(0).astype(int)
            pregao['DataNegocio'] = pd.to_datetime(pregao['DataNegocio']).astype('datetime64[ms]')
            pregao['CodigoParticipanteComprador'] = pregao['CodigoParticipanteComprador'].fillna(0).astype(int)
            pregao['CodigoParticipanteVendedor'] = pregao['CodigoParticipanteVendedor'].fillna(0).astype(int)

            # Converter o DataFrame para um Table do PyArrow
            table = pa.Table.from_pandas(pregao)

            
            # Gerar a partição diária
            data_atual_str = datetime.date.today().strftime('%Y-%m-%d')
            parquet_s3_path = f'{s3_path}data_atual={data_atual_str}/bovespa.parquet'
            
            # Gravar o Parquet diretamente no S3 em memória usando BytesIO
            print(f"Salvando chunk diretamente no S3 em {parquet_s3_path}...")
            with BytesIO() as parquet_buffer:
                pq.write_table(table, parquet_buffer, compression='snappy')
                parquet_buffer.seek(0)  # Voltar ao início do buffer
                
                # Fazer o upload do buffer Parquet diretamente para o S3
                with s3_fs.open(f's3://{bucket_name}/{parquet_s3_path}', 'wb') as s3_file:
                    s3_file.write(parquet_buffer.read())
            
            print(f"Upload do chunk {i} concluído com sucesso.")