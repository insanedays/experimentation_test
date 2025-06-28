# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract data
# MAGIC Esse notebook realiza extração dos dados de um s3 e armazena em parquet.
# MAGIC
# MAGIC ##### Práticas adotadas:
# MAGIC - Leitura e gravação utilizando chunks para maior eficiência
# MAGIC - Armazenamento organizado em diretórios distintos dos dados brutos
# MAGIC - Conversão para parquet utilizando Snappy compression que é colunar para leitura e análise mais eficiente 
# MAGIC - Uso do Unity Catalog
# MAGIC - Ajuste dinâmico dos caminhos via json
# MAGIC - Impressão do schema dos dados

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Preparando ambiente
# MAGIC
# MAGIC - Realização dos imports
# MAGIC - Criação dos diretórios 
# MAGIC - Configuração dinâmica dos diretórios com base em um arquivo `env.json`

# COMMAND ----------

import os
import logging
import pandas as pd
import requests
import gzip
import tarfile
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
import sys
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path.cwd().parent
sys.path.append(str(ROOT))

from utilis.config import create_volumes_from_config, get_paths

# COMMAND ----------

# loadin spark
try:
    spark
except NameError:
    spark = SparkSession.builder.getOrCreate()

# creating volumes in unity caralog
create_volumes_from_config(spark) 

# COMMAND ----------

# loading diretorys paths
paths = get_paths()
raw_dir = paths["RAW_DIR"]
temp_dir = paths["TMP_DIR"]

# COMMAND ----------

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# COMMAND ----------

os.makedirs(temp_dir, exist_ok=True)
os.makedirs(raw_dir, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Extração e descompressão

# COMMAND ----------

# data shared 
DATA = {
    "order": {
        "url": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz",
        "type": "json"
    },
    "consumer": {
        "url": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz",
        "type": "csv"
    },
    "restaurant": {
        "url": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz",
        "type": "csv"
    },
    "ab_test": {
        "url": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz",
        "type": "tar"
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.1 Funções

# COMMAND ----------

# reading the content in chunks to avoid memory bottlenecks during download
# here i am not using Unity Catalog because it does not support .gz files
def download_data(file_key: str, destination_path: str = temp_dir, timeout: int = 30) -> str:
    """
    Baixa um arquivo a partir de uma chave presente no dicionário DATA
    e salva no diretório informado com o nome baseado em file_key.

    Args:
        file_key (str): Chave no dicionário DATA, onde está a URL do arquivo.
        destination_path (str): Diretório onde o arquivo será salvo.
        timeout (int): Tempo máximo (em segundos) para aguardar a resposta da URL.

    Returns:
        str: Caminho completo do arquivo salvo.
    """
    if file_key not in DATA:
        raise ValueError(f"Chave '{file_key}' não encontrada em DATA.")

    try:
        url = DATA[file_key]["url"]
        os.makedirs(destination_path, exist_ok=True)

        file_name = os.path.basename(url)
        full_path = os.path.join(destination_path, file_name)

        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()

        with open(full_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        if not os.path.exists(full_path) or os.path.getsize(full_path) == 0:
            logger.error(f"[Download] Arquivo baixado vazio ou inexistente em '{full_path}': {str(e)}")
            raise RuntimeError(f"[Download] Arquivo baixado vazio ou inexistente em {full_path}")

        logger.info(f"Arquivo '{file_key}' baixado com sucesso para {full_path}")
        return full_path

    except Exception as e:
        logger.error(f"[Download] Falha ao baixar '{file_key}' ({url}): {str(e)}")
        raise RuntimeError(f"[Download] Falha ao baixar '{file_key}' ({url}): {str(e)}")


# COMMAND ----------

def decompress_gz(file_path: str)-> str:
    """
    Descompacta um arquivo .gz e salva o conteúdo no mesmo diretório.

    Args:
        file_path (str): Diretório onde o arquivo está salvo.

    Returns:
        str: Caminho completo do arquivo descompactado.

    Raises:
        Exception: Caso ocorra alguma falha durante a descompactação.
    """
    try:
        output_name = os.path.splitext(os.path.basename(file_path))[0]
        output_path = os.path.join(os.path.dirname(file_path), output_name)

        with gzip.open(file_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
  
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            logger.error(f"[Descompress] Não encontrado ou vazio {output_path}")
            raise RuntimeError(f"[Descompress] Arquivo extraído não encontrado ou vazio: {output_path}")
        
        os.remove(file_path)
        logger.info(f"[Descompress] Arquivo '{output_name}' descompactado com sucesso em {output_path}")
        return output_path
    
    except Exception as e:
        logger.error(f"[Descompress] Falha no arquivo '{file_path}': {str(e)}")
        raise RuntimeError(f"[Descompress] Falha no arquivo '{file_path}'") from e

# COMMAND ----------

def decompress_tar_gz(tar_path: str) -> str:
    """
    Extrai apenas o segundo arquivo de um .tar.gz, ignorando hierarquia de pastas.
    O arquivo extraído será salvo no mesmo diretório do .tar.gz.
    O .tar.gz original é removido apenas se o arquivo for extraído corretamente.

    Args:
        tar_path (str): Caminho do arquivo .tar.gz.

    Returns:
        str: Caminho completo do arquivo extraído.

    Raises:
        RuntimeError: Em caso de erro na extração.
    """
    try:
        extract_dir = os.path.dirname(tar_path)

        with tarfile.open(tar_path, 'r:gz') as tar:
            files = [m for m in tar.getmembers() if m.isfile()]
            if not files:
                raise ValueError(f"O arquivo tar '{tar_path}' não contém arquivos válidos.")

            first_file = files[1]
            original_name = os.path.basename(first_file.name)
            first_file.name = original_name

            output_path = os.path.join(extract_dir, original_name)
            tar.extract(first_file, path=extract_dir)


        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            logger.error(f"[Descompress] Não encontrado ou vazio {output_path}")
            raise RuntimeError(f"[Descompress]Arquivo extraído não encontrado ou vazio: {output_path}")
     
        os.remove(tar_path)
        logger.info(f"{original_name} extraído com sucesso em: {output_path}")
        
        return output_path

    except Exception as e:
        logger.error(f"[Descompress] Erro ao extrair '{tar_path}': {str(e)}")
        raise RuntimeError(f"[Descompress] Falha na extração de '{tar_path}': {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC #####  2.2 Extraindo e descompactando os dados

# COMMAND ----------

order_dir = decompress_gz(download_data('order'))
consumer_dir = decompress_gz(download_data('consumer'))
restaurant_dir = decompress_gz(download_data('restaurant'))
ab_test_dir = decompress_tar_gz(download_data('ab_test'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Convertendo em .parquet

# COMMAND ----------

#
files_dir = {
    "order": order_dir,
    "consumer": consumer_dir,
    "restaurant": restaurant_dir,
    "ab_test": ab_test_dir
}

for nome, caminho in files_dir.items():
    tamanho_mb = round(os.path.getsize(caminho) / (1024 * 1024), 2)
    print(f"Tamanho de {nome}: {tamanho_mb} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.1 Funções

# COMMAND ----------

def json_to_parquet_chunks(
    json_path: str ,
    parquet_path: str = os.path.join(raw_dir, "order.parquet")  ,
    chunksize: int = 100_000,
    compression: str = "snappy"
) -> None:
    """
    Converte um arquivo JSON grande em um único arquivo .parquet,
    processando em chunks para uso eficiente de memória.
    Cada chunk é lido do JSON e gravado diretamente no arquivo .parquet final,
    sem manter todos os dados carregados simultaneamente em memória.

    Args:
        json_path (str): Caminho para o arquivo JSON no formato NDJSON (um JSON por linha).
        parquet_path (str): Caminho onde o arquivo Parquet final será salvo.
        chunksize (int): Número de linhas por chunk durante a leitura (default: 100_000).
        compression (str): Algoritmo de compressão Parquet a ser utilizado (default: "snappy").

    Raises:
        RuntimeError: Em caso de falha na leitura do JSON ou escrita do Parquet.
    """
    writer = None

    for i, chunk in enumerate(pd.read_json(json_path, lines=True, chunksize=chunksize)):

        for col in chunk.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns: # spark cant read ns
            chunk[col] = pd.to_datetime(chunk[col]).dt.date

        table = pa.Table.from_pandas(chunk, preserve_index=False)
 
        if writer is None:
            writer = pq.ParquetWriter(parquet_path, table.schema, compression="snappy")

        writer.write_table(table)

    if writer:
        writer.close()


# COMMAND ----------

def csv_to_parquet(file_name, csv_path: str, parquet_path: str = raw_dir) -> None:
    """
    Converte um arquivo CSV em um arquivo Parquet.

    Args:
        csv_path (str): Caminho para o arquivo CSV.
        parquet_path (str): Caminho onde o arquivo Parquet final será salvo.
    """
    try:
        df = pd.read_csv(csv_path)
        df.to_parquet(os.path.join(parquet_path,f"{file_name}.parquet"), engine="pyarrow", compression="snappy")
        display(df)
        print(f"Arquivo Parquet salvo em: {parquet_path}")
    except Exception as e:
        print(f"Erro ao converter CSV para Parquet: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.2 Executando funções

# COMMAND ----------

for name, file_dir in files_dir.items():
    if file_dir.endswith('.csv'):
        csv_to_parquet(name,file_dir)
        print(file_dir)
    else:
        json_to_parquet_chunks(file_dir)

# COMMAND ----------

# for file in os.listdir(raw_dir):
#     file_path = os.path.join(raw_dir, file)
#     if os.path.isfile(file_path) and not file.endswith(".parquet"):
#         os.remove(file_path)