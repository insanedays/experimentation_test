# Databricks notebook source
# MAGIC %md
# MAGIC ## Pré processamento
# MAGIC Este notebook realiza o pré-processamento inicial dos dados com os seguintes objetivos:
# MAGIC
# MAGIC - Ajustar os tipos de dados conforme necessário;
# MAGIC - Unificar tabelas que devem ser analisadas de forma integrada;
# MAGIC - Avaliar os campos disponíveis de cada fonte;
# MAGIC - Selecionar os campos relevantes para a análise, com justificativas alinhadas ao objetivo analítico;
# MAGIC - Salvar os dados na camada refined;
# MAGIC - Adicionar partição por created_at na tabela order para otimizar consultas futuras.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configs

# COMMAND ----------

!pip install pandasql

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
import sys
from pathlib import Path
import os
import json

ROOT = Path.cwd().parent
sys.path.append(str(ROOT))

from utilis.config import get_paths

# COMMAND ----------

spark = SparkSession.builder.appName("ParquetReader").getOrCreate()

paths = get_paths()
raw_dir = paths["RAW_DIR"]
refined_dir = paths["REFINED_DIR"]
temp_dir = paths["TMP_DIR"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Carregando os dados
# MAGIC

# COMMAND ----------

def get_size_mb(path):
    """Retorna o tamanho de um arquivo em MB."""
    return os.path.getsize(path) / (1024 ** 2)

consumer_path = f"{raw_dir}/consumer.parquet"
restaurant_path = f"{raw_dir}/restaurant.parquet"
ab_test_path = f"{raw_dir}/ab_test.parquet"
order_path = f"{raw_dir}/order.parquet"

print("Tamanhos dos arquivos Parquet (em MB):")
print(f"consumer  : {get_size_mb(consumer_path):.2f} MB")
print(f"restaurant: {get_size_mb(restaurant_path):.2f} MB")
print(f"ab_test   : {get_size_mb(ab_test_path):.2f} MB")
print(f"order     : {get_size_mb(order_path):.2f} MB")


# COMMAND ----------

consumer = pd.read_parquet(consumer_path)
restaurant = pd.read_parquet(restaurant_path)
ab_test = pd.read_parquet(ab_test_path)
order = spark.read.parquet(order_path) # using spark 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Refinamento dos dados
# MAGIC
# MAGIC Análise preliminar das variáveis para seleção dos campos relevantes à etapa analítica. Tudo que for classificado como irrelevante ou baixo não está utilizado nessa primeira análise.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Consumer
# MAGIC
# MAGIC
# MAGIC | Campo                   | Decisão      | Justificativa                                                            |
# MAGIC |-------------------------|------------------|----------------------------------------------------------------------------------------|
# MAGIC | `customer_id`           | Muito Alta        | Chave estrangeira para identificar os targets           |
# MAGIC | `language`              | Irrelevante      | Quase todos os registros são `pt-br` não contribuindo para segmentação ou análise      |
# MAGIC | `created_at`            | Alta       | Permite estimar o tempo como cliente que pode influenciar diretamente na análise. Pode afetar tanto a propensão à resposta quanto o comportamento de recompra.  |
# MAGIC | `active`                | Baixa        | Pode ser usada como métrica de conversão ou filtro para participantes válidos. Porém são menos de 2% dos dados, por isso a classificação baixa. Vou manter para avaliar melhor o dado       |
# MAGIC | `customer_name`         | Irrelevante      | Dados pessoais não informam comportamento ou impacto no experimento.                 |
# MAGIC | `customer_phone_area`   | Irrelevante      | Informação regional indireta pouco confiável                                     |
# MAGIC | `customer_phone_number` | Irrelevante      | Não agrega valor analítico para o teste                      |
# MAGIC

# COMMAND ----------

consumer.info()

# COMMAND ----------

consumer.sample(4)

# COMMAND ----------

consumer["active"].value_counts(dropna=False)

# COMMAND ----------

consumer["language"].value_counts(dropna=False)

# COMMAND ----------

consumer["active"].value_counts(dropna=False) 

# COMMAND ----------

consumer_processed = consumer.copy()

consumer_processed["created_at"] = pd.to_datetime(consumer_processed["created_at"], utc=True)

consumer_processed["created_date"] = consumer_processed["created_at"].dt.date

# COMMAND ----------

colums_to_mantain = [
    "customer_id",
    "active",
    "created_date",
]

consumer_processed = consumer_processed[colums_to_mantain]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Order
# MAGIC
# MAGIC
# MAGIC | Campo                         | Relevância       | Justificativa                                                                  |
# MAGIC |------------------------------|------------------|----------------------------------------------------------------------------------------|
# MAGIC | `cpf`                        | Irrelevante      | Dado sem valor analítico direto para segmentação ou avaliação.      |
# MAGIC | `customer_id`                | Alta        | Chave estrangeira de ligação com a tabela de clientes                                                |
# MAGIC | `customer_name`              | Irrelevante      | Informações pessoal, não agrega valor à análise comportamental.                      |
# MAGIC | `delivery_address_city`      | Alta        | Útil para controle de localização ou segmentação regional.                            |
# MAGIC | `delivery_address_country`   | Irrelevante      | Todos os dados são do Brasil                           |
# MAGIC | `delivery_address_district`  | Baixa         | Talvez fizesse sentido em uma segunda análise caso fosse necessário entrar no micro depois de avaliar as demais granularidades
# MAGIC | `delivery_address_external_id` | Irrelevante    | Identificador sem valor para o teste              |
# MAGIC | `delivery_address_latitude`  | Baixa    | Permite análises espaciais detalhadas, porém utilizarei cidade e estado apenas          |
# MAGIC | `delivery_address_longitude` | Baixa       | Complementa a latitude para segmentação espacial ou análise de dispersão.             |
# MAGIC | `delivery_address_state`     | Alta        | Permite controle ou análise por unidade federativa, boa granularidade intermediária.  |
# MAGIC | `delivery_address_zip_code`  | Irrelevante      | Não faz sentido converter o merchant_timezone pois o objetivo é entender o comportamento de consumo local.
# MAGIC | `items`                      | Baixa      | Mais útil apra entender o perfil de consumo, não possuí evidência de uso de cupons nos dados
# MAGIC | `merchant_id`                | Muito alta        | Chave estrangeira           |
# MAGIC | `merchant_latitude`          | Baixa          | Não faz sentido para essa etapa da análse, distância entre o consumidor e o restaurante não parece relevante |
# MAGIC | `merchant_longitude`         | Baixa           | Mesmo caso da latitude.                                                               |
# MAGIC | `merchant_timezone`          | Irrelevante      | Supondo que o país é único, não há variação suficiente para impacto.                  |
# MAGIC | `order_created_at`           | Muito alta   | Define a data da compra, essencial para delimitar janelas de experimento.             |
# MAGIC | `order_id`                   | Relevante        | Identificador transacional para rastreio de conversões por grupo de teste.            |
# MAGIC | `order_scheduled`            | Baixo         | Apenas 1% dos dados, considero irrisório para gerar alguma classificação do cliente    |
# MAGIC | `order_total_amount`         | Muito alta        | Métrica de conversão principal: valor monetário associado ao comportamento.           |
# MAGIC | `origin_platform`            | Moderada        | Permite segmentar por canal.                                                          |
# MAGIC | `order_scheduled_date`       | Irrelevante      | Redundante com `order_scheduled`. Só serviria para entender se temos clientes que agendaram antes da campanha começar.                  |
# MAGIC

# COMMAND ----------

order.printSchema()
print(f"Total de linhas: {order.count()}")

# COMMAND ----------

order.select("*").limit(1).toPandas()

# COMMAND ----------

order.groupBy("delivery_address_country").count().show()

# COMMAND ----------

order.groupBy("origin_platform").count().show()

# COMMAND ----------

order.groupBy("order_scheduled").count().show()

# COMMAND ----------

columns = [
    "customer_id"
    , "delivery_address_city"
    # , "delivery_address_district"
    # , "delivery_address_latitude"
    # , "delivery_address_longitude"
    , "delivery_address_state"
    , "items"
    , "merchant_id"
    # , "merchant_latitude"
    # , "merchant_longitude"
    , "order_created_at"
    , "order_id"
    # , "order_scheduled"
    , "order_total_amount"
    , "origin_platform"
]

order_processed = order.select(*columns)

# COMMAND ----------

items = order_processed.select("order_id", "items")
order_processed = order_processed.drop("items")

# COMMAND ----------

row = items.select("*").take(1)[0]
items_json = row["items"]

parsed = json.loads(items_json) if isinstance(items_json, str) else items_json
print(json.dumps(parsed, indent=2, ensure_ascii=False))

# COMMAND ----------

items.select("order_id", "items").createOrReplaceTempView("items_view")

# COMMAND ----------

# MAGIC %%sql
# MAGIC WITH parsed_items AS (
# MAGIC   SELECT
# MAGIC     order_id
# MAGIC     , EXPLODE(
# MAGIC         FROM_JSON(
# MAGIC             items
# MAGIC             , 'array<struct<totalDiscount:struct<value:string>>>'
# MAGIC         )
# MAGIC     ) AS item
# MAGIC FROM items_view
# MAGIC )
# MAGIC SELECT
# MAGIC     order_id
# MAGIC     , CAST(item.totalDiscount.value AS DOUBLE)  AS total_discount
# MAGIC     , CASE 
# MAGIC         WHEN CAST(item.totalDiscount.value AS DOUBLE) > 0 THEN 1 ELSE 0
# MAGIC         END AS has_discount
# MAGIC FROM parsed_items

# COMMAND ----------

# MAGIC %md
# MAGIC #### Restaurant
# MAGIC
# MAGIC
# MAGIC | Campo                   | Relevância       | Justificativa                                                             |
# MAGIC |-------------------------|------------------|----------------------------------------------------------------------------------------|
# MAGIC | `id`                   | Alta        | Chave importante para conectar as tabelas   |
# MAGIC | `created_at`           | Irrelevante        | Estamos análisando comportamento do cliente |
# MAGIC | `enabled`              | Irrelevante        | Estamos análisando comportamento do cliente . Não importa se o restaurante está ativo ou inativo  |
# MAGIC | `price_range`          | Baixa       | Pode indicar posicionamento de mercado podendo, porém é mais seguro avaliar pelas compras do cliente por isso baixo |
# MAGIC | `average_ticket`       | Moderada        | Ideal é utilizar o ticket médio do cliente, mas irei deixar disponível caso precise aprofundar em algo    |
# MAGIC | `takeout_time`         | Baixa     | Ideal seria ter o tempo que demorou o pedido  |
# MAGIC | `delivery_time`        | Baixa        | Indicador de logística que pode influenciar na conversão do cliente, porém não temos o tempo que a entrega chegou para avaliar o tempo de entrega e comparar, por isso  está classificado como como baixa              |
# MAGIC | `minimum_order_value`  | Baixa        | Mais relevante a perfil de compra, temos já o valor da order  |
# MAGIC | `merchant_zip_code`    | Irrelevante  | Manterei apenas o do cliente |
# MAGIC | `merchant_city`        | Irrelevante        |  Manterei apenas o do cliente                |
# MAGIC | `merchant_state`       | Irrelevante        |  Manterei apenas o do cliente               |
# MAGIC | `merchant_country`     | Irrelevante      | Apenas Brasil     |
# MAGIC

# COMMAND ----------

restaurant.info() 

# COMMAND ----------

restaurant.sample(4)

# COMMAND ----------

restaurant_processed = restaurant.copy()
restaurant_processed["created_date"] = pd.to_datetime(restaurant_processed["created_at"]).dt.date

# COMMAND ----------

columns_to_mantain = [
    'id'
   # ,'created_date'
   # ,'enabled'
   # , 'price_range'
    ,'average_ticket'
   # ,'takeout_time'
   # , 'delivery_time'
   # ,'minimum_order_value'
]
restaurant_processed = restaurant_processed[columns_to_mantain]

# COMMAND ----------

# MAGIC %md
# MAGIC #### AB Test

# COMMAND ----------

ab_test.info()
ab_test.sample(4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Salvando parquets na camada refined
# MAGIC

# COMMAND ----------

# I will not use items it contains too much data with low relevance for the analysis.
consumer_processed.to_parquet(f"{refined_dir}/consumer.parquet", index=False, engine="pyarrow", compression="snappy")
restaurant_processed.to_parquet(f"{refined_dir}/restaurant.parquet", index=False, engine="pyarrow", compression="snappy")
ab_test.to_parquet(f"{refined_dir}/ab_test.parquet", index=False, engine="pyarrow", compression="snappy")

print("[Pandas] consumer, restaurant e ab_test salvos com sucesso.")

# saving with spark and partitionBy
order_processed.write \
    .mode("overwrite") \
    .partitionBy("order_created_at") \
    .parquet(f"{refined_dir}/order")

print("[Spark] order salvo com particionamento por 'order_created_at'.")

# COMMAND ----------


consumer_path = f"{refined_dir}/consumer.parquet"
restaurant_path = f"{refined_dir}/restaurant.parquet"
ab_test_path = f"{refined_dir}/ab_test.parquet"
order_path = f"{refined_dir}/order"


print("Tamanhos dos arquivos Parquet (em MB):")
print(f"consumer  : {get_size_mb(consumer_path):.2f} MB")
print(f"restaurant: {get_size_mb(restaurant_path):.2f} MB")
print(f"ab_test   : {get_size_mb(ab_test_path):.2f} MB")
print(f"order     : {get_size_mb(order_path):.2f} MB")

# COMMAND ----------

# testing 
order_df = pd.read_parquet(order_path, engine="pyarrow")

# COMMAND ----------

!pip install pandasql

# COMMAND ----------

# order_df.createOrReplaceTempView("order")
# consumer_df.createOrReplaceTempView("consumer")
# ab_test_df.createOrReplaceTempView("ab_test")
# restaurant_df.createOrReplaceTempView("restaurant")

# COMMAND ----------

import duckdb
con = duckdb.connect()

con.register("ab_test_df", ab_test) 
con.register("order_df", order_df)
con.register("restaurant_df", restaurant_processed)
con.register("consumer_df", consumer_processed)

# COMMAND ----------

import duckdb

execute = """
SELECT 
      o.customer_id
    , o.order_id
    , o.delivery_address_city            AS order_city
    , o.delivery_address_state           AS order_state
    , o.order_total_amount               AS order_amount
    , o.origin_platform                  AS order_origin_platform
    , CAST(o.order_created_at AS DATE)   AS order_date
    , c.active                           AS custumer_active
    , CAST(c.created_date AS DATE)       AS custumer_created_date
    , r.average_ticket                   AS restaurante_average_ticket
    , CASE WHEN a.is_target = 'Target' THEN 1 ELSE                         AS custumer_is_target
FROM order_df o
LEFT JOIN consumer_df c ON c.customer_id = o.customer_id
LEFT JOIN restaurant_df r ON r.id = o.merchant_id 
JOIN ab_test_df a ON a.customer_id = o.customer_id 
"""

obt_coupon_analysis = con.execute(execute).df()



# COMMAND ----------

consumer_processed.to_parquet(
    f"{refined_dir}/obt_coupon_analysis.parquet",
    index=False,
    engine="pyarrow",
    compression="snappy",
    partition_cols=["order_date","custumer_is_target"]
)

# COMMAND ----------

# from pandasql import sqldf
# import traceback

# # Definir executor
# pysqldf = lambda q: sqldf(q, globals())

# # Escreva sua query com os nomes reais dos DataFrames (exatos!)
# query = """
# SELECT 
#       o.customer_id
#     , o.order_id
#     , o.delivery_address_city    AS order_city
#     , o.delivery_address_state   AS order_state
#     , o.order_total_amount       AS order_amount
#     , o.origin_platform          AS order_origin_platform
#     , o.order_created_at         AS order_date
#     , c.active                   AS custumer_active
#     , c.created_date             AS custumer_created_date
#     , at.is_target               AS custumer_is_target
#     , r.average_ticket           AS restaurante_average_ticket
# FROM order_df o
# LEFT JOIN consumer c USING(customer_id)
# LEFT JOIN ab_test at USING(customer_id)
# LEFT JOIN restaurant r ON r.restaurant_id = o.merchant_id 
# """

# try:
#     obt_coupon_analysis = pysqldf(query)
# except Exception as e:
#     print("Erro ao executar a query:")
#     print(type(e).__name__)  # Tipo do erro, ex: ObjectNotExecutableError
#     print(str(e))            # Mensagem detalhada do erro
#     traceback.print_exc()