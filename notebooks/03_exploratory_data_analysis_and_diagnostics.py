# Databricks notebook source
# MAGIC %md
# MAGIC ### Configurações do ambiente
# MAGIC
# MAGIC - pip install
# MAGIC - Import dos dados (sns.set, display options, etc.)
# MAGIC - Inicializaçao do spark  
# MAGIC - Carregando caminhos do projeto
# MAGIC - Carregamento de dados

# COMMAND ----------

!pip install duckdb

# COMMAND ----------

import calendar

# system and paths
import os
import sys
from pathlib import Path

# Viz
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import FuncFormatter

# data processing
import pandas as pd
from pyspark.sql import SparkSession
import duckdb
import math

# load project paths
ROOT = Path.cwd().parent
sys.path.append(str(ROOT))

from utilis.config import get_paths

spark = SparkSession.builder.appName("ParquetReader").getOrCreate()

# COMMAND ----------

spark = SparkSession.builder.appName("ParquetReader").getOrCreate()

paths = get_paths()
raw_dir = paths["RAW_DIR"]
refined_dir = paths["REFINED_DIR"]
temp_dir = paths["TMP_DIR"]
curated_dir = paths["CURATED_DIR"] 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnostico inicial dos dados
# MAGIC
# MAGIC
# MAGIC - Verificação da dimensão (linhas e colunas)
# MAGIC - Verificação dos tipos de dados
# MAGIC - Verificação de valores nulos
# MAGIC - Checagem de duplicatas
# MAGIC - Visualização de uma amostra dos dados 
# MAGIC - Estatísticas descritivas das colunas numéricas e categoricas
# MAGIC - Ajustes
# MAGIC - Junções
# MAGIC
# MAGIC

# COMMAND ----------

consumer_path = f"{refined_dir}/consumer.parquet"
restaurant_path = f"{refined_dir}/restaurant.parquet"
ab_test_path = f"{refined_dir}/ab_test.parquet"
order_path = f"{refined_dir}/order"

consumer_df = pd.read_parquet(consumer_path)
restaurant_df = pd.read_parquet(restaurant_path)
ab_test_df = pd.read_parquet(ab_test_path)
order_df = pd.read_parquet(order_path)  

# COMMAND ----------

order_df.info()

# COMMAND ----------

order_df["order_created_at"] = pd.to_datetime(order_df["order_created_at"].astype(str), errors="coerce").dt.date

# COMMAND ----------

print(f' min_order_date: {order_df["order_created_at"].min()}')
print(f' min_order_date: {order_df["order_created_at"].max()}')

# COMMAND ----------

restaurant_df.info()

# COMMAND ----------

import duckdb
con = duckdb.connect()

con.register("ab_test_df", ab_test_df) 
con.register("order_df", order_df)
con.register("restaurant_df", restaurant_df)
con.register("costumer_df", consumer_df)



# COMMAND ----------

execute = """
SELECT 
      o.customer_id
    , o.order_id
    , o.delivery_address_city            AS order_city
    , o.delivery_address_state           AS order_state
    , o.order_total_amount               AS order_amount
    , o.origin_platform                  AS order_origin_platform
    , CAST(o.order_created_at AS DATE)   AS order_date
    , CASE WHEN c.active = TRUE THEN 1
        WHEN c.active = FALSE THEN 0 END AS custumer_active
    , CAST(c.created_date AS DATE)       AS custumer_created_date
    , r.average_ticket                   AS restaurante_average_ticket
    , CASE WHEN a.is_target = 'control' 
          THEN 0 
        WHEN a.is_target = 'target' THEN 1 END AS custumer_is_target
FROM order_df o
LEFT JOIN costumer_df c ON c.customer_id = o.customer_id
LEFT JOIN restaurant_df r ON r.id = o.merchant_id 
JOIN ab_test_df a ON a.customer_id = o.customer_id 
"""

obt_coupon_analysis = con.execute(execute).df()

# COMMAND ----------

con.register("obt_coupon_analysis_db", obt_coupon_analysis)

# COMMAND ----------

obt_coupon_analysis.head(3)

# COMMAND ----------

obt_coupon_analysis.info()

# COMMAND ----------

print("Dimensão do DataFrame:")
print(obt_coupon_analysis.shape)

print("\nPercentual de valores nulos por coluna:")
nulls = obt_coupon_analysis.isnull().mean().sort_values(ascending=False) * 100
print(f'{nulls[nulls > 0].round(2)}%')

duplicated_rows = obt_coupon_analysis.duplicated().sum()
print(f"\nNúmero de linhas duplicadas: {duplicated_rows}")


# COMMAND ----------

print("\nDistribuição das principais categorias:")
cat_cols = obt_coupon_analysis.select_dtypes(include='object').columns
cat_cols = [col for col in cat_cols if 'id' not in col.lower()]  #removing ids columns
for col in cat_cols:
    print(f"\n{col} ({obt_coupon_analysis[col].nunique()} categorias)")
    print(obt_coupon_analysis[col].value_counts(normalize=True).head(5).round(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação de métricas derivadas
# MAGIC
# MAGIC As seguintes métricas foram construídas para descrever o comportamento dos clientes ao longo do período analisado:
# MAGIC
# MAGIC - **customer monetary**: gasto total por cliente  
# MAGIC   `SUM(order_total_amount)`
# MAGIC
# MAGIC - **customer average ticket**: ticket médio do cliente  
# MAGIC   `SUM(order_total_amount) / COUNT(order_id)`
# MAGIC
# MAGIC - **customer first purchase**: data do primeiro pedido do cliente  
# MAGIC   `MIN(order_date)`
# MAGIC
# MAGIC - **customer last purchase**: data do último pedido do cliente  
# MAGIC   `MAX(order_date)`
# MAGIC
# MAGIC - **customer recency**:  tempo desde o último pedido até a ultima data de pedido contida nos dados  
# MAGIC   `2018-12-03 - MAX(order_date)`
# MAGIC
# MAGIC - **customer age**: tempo como cliente até o início da promoção  
# MAGIC   `data_início_promoção(2019-01-31) - customer_create_date`
# MAGIC
# MAGIC - **customer frequency**: quantidade de pedidos por cliente  
# MAGIC   `COUNT(order_id)`
# MAGIC
# MAGIC - **customer lifetime range**: tempo de vida do cliente na janela analisada  
# MAGIC   `MAX(order_date) - MIN(order_date)`
# MAGIC
# MAGIC   No decorrer da análise também defini outras métricas que mudam as variaveis originais por dia em grão de semana
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

order_metrics = """
    SELECT
        customer_id
        , custumer_is_target
        , SUM(order_amount)                  AS customer_monetary
        , COUNT(DISTINCT order_id)           AS customer_frequency
        , SUM(order_amount)
            / COUNT(DISTINCT order_id)       AS customer_average_ticket
        , MIN(order_date)                    AS customer_first_purchase
        , MAX(order_date)                    AS customer_last_purchase
        , CAST(MAX(order_date) AS DATE)
         -  CAST(MIN(order_date)AS DATE)     AS customer_lifetime_range 
        , MAX(
            (CAST('2018-12-03' AS DATE)
            - CAST(custumer_created_date AS DATE)
            ))                                AS customer_age
        , MIN(
            CAST('2019-01-31' AS DATE)  
            -  CAST(order_date AS DATE))     AS customer_recency
    FROM obt_coupon_analysis 
    GROUP BY 1, 2
"""

order_metrics = con.execute(order_metrics).df()

# COMMAND ----------

order_metrics.describe()

# COMMAND ----------

con.register("order_metrics", order_metrics) 

# COMMAND ----------

metrics = [
    "customer_monetary"
    , "customer_frequency"
    , "customer_average_ticket"
    , "customer_first_purchase"
    , "customer_last_purchase"
    , "customer_lifetime_range"
    , "customer_age"
    , "customer_recency"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identificando grupos A e B
# MAGIC
# MAGIC - Qual a quantidade de clientes por grupo?
# MAGIC - Existem clientes em ambos os grupos?

# COMMAND ----------

total_customers = ab_test_df["customer_id"].nunique()

control_customers = ab_test_df[ab_test_df["is_target"] == 'control']["customer_id"].unique()
treatment_customers = ab_test_df[ab_test_df["is_target"] == 'target']["customer_id"].unique()

# counts
num_control = len(control_customers)
num_treatment = len(treatment_customers)
num_overlap = len(set(control_customers) & set(treatment_customers))

# results
print(f"Total de consumidores: {total_customers}")
print(f"Customers no grupo de controle: {num_control}")
print(f"Consumidores no grupo de tratamento: {num_treatment}")
print(f"Consumidores em ambos grupos: {num_overlap}")


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Avaliando distribuição das métricas entre os grupos
# MAGIC Como várias dessas variáveis apresentaram distribuições assimétricas com presença de outliers extremos acima do terceiro quartil (75%), utilizei escala logarítmica no eixo Y para melhorar a legibilidade e permitir uma me melhor comparação etre os grupos controle e teste. A assimetria positiva se mantém mesmo após a transformação logarítmica, mas a visualização torna-se mais interpretável.
# MAGIC

# COMMAND ----------

to_remove = ["customer_first_purchase", "customer_last_purchase"]

temporary_var = [item for item in metrics if item not in to_remove]
#number of graphics
n = len(temporary_var)
cols = 3
rows = math.ceil(n / cols)

plt.figure(figsize=(6 * cols, 4 * rows))
plt.suptitle("Distribuição das Métricas por Grupo (Controle vs. Target)", fontsize=16, y=1.02)

for i, var in enumerate(temporary_var, 1):
    plt.subplot(rows, cols, i)

    sns.boxplot(
        data=order_metrics,
        x="custumer_is_target",
        y=var,
        medianprops=dict(color="indianred", linewidth=2),
        palette="pastel"
    )
# assimetrics
    plt.yscale("log")  
    plt.title(f"{var.replace('_', ' ').title()}")
    plt.xlabel("Grupo")
    plt.ylabel(var.replace('_', ' ').title())
    plt.grid(True, axis="y", linestyle="--", alpha=0.4)


plt.tight_layout(rect=[0, 0, 0.95, 0.95]) 
plt.savefig(f"../img/metricas_boxplot.png")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Ao comparar os dois grupos, observamos que o grupo** 1 (target) apresenta maior tempo de vida como cliente** dentro da janela analisada, além de uma **mediana mais alta no tempo desde a criação da conta até o início da campanha**, indicando que são clientes mais antigos na base. Também se destaca por uma **frequência de pedidos ligeiramente maior**, sugerindo maior engajamento. Apesar disso não há diferença significativa no ticket médio entre os grupos, o que indica que embora os clientes do grupo target comprem com mais frequência, o valor médio gasto por pedido se mantém estável em relação ao grupo de controle.
# MAGIC
# MAGIC Já o grupo 0 (controle) apresenta maior recência de compra, ou seja, está mais distante da última data de compra no período analisado. A distribuição de recência para esse grupo mostra uma assimetria mais acentuada, com uma cauda longa próxima de zero, sugerindo que há uma parcela de clientes que ainda realizou pedidos recentemente, mas uma parte significativa está mais inativa. No grupo target, essa distribuição é mais concentrada, indicando que os clientes tendem a ter mantido um comportamento de compra mais consistente até o fim da base analisada.

# COMMAND ----------

# MAGIC %md
# MAGIC Optei por nao remover os outlier com a pressima que não é inconsistência dos dados, nao acredito que tira-lo vai melhorar de alguma forma a analise. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Avaliação de valor e frequência de pedidos
# MAGIC
# MAGIC - Evolução diária da  frequência e valor total de pedidos
# MAGIC - Evolução diária de frequencia e valor total normalizado pela quantidade de clientes
# MAGIC - Pedidos acumulados por dia
# MAGIC - Diferença de evolução semanal de frequência e valor total 
# MAGIC - teste p
# MAGIC - uplift
# MAGIC
# MAGIC

# COMMAND ----------

df_daily = (
    obt_coupon_analysis
    .groupby([obt_coupon_analysis["order_date"].dt.date, "custumer_is_target"])
    .agg(
        order_count=('order_id', 'count'),
        total_amount=('order_amount', 'sum'),
        unique_customers=('customer_id', 'nunique')
    )
    .reset_index()
    .rename(columns={'order_date': 'date'})
)




# COMMAND ----------

df_daily["order_weekday"] = df_daily["date"].apply(str).apply(pd.to_datetime).dt.day_name()


# COMMAND ----------

df_daily.head(3)

# COMMAND ----------

plt.figure(figsize=(12, 5))
sns.lineplot(data=df_daily, x='date', y='order_count', hue='custumer_is_target', marker='o')
plt.title("Evolução diária da frequência de pedidos por grupo")
plt.xlabel("Data")
plt.ylabel("Número de pedidos")
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


# COMMAND ----------

plt.figure(figsize=(12, 5))
sns.lineplot(data=df_daily, x='date', y='total_amount', hue='custumer_is_target', marker='o')
plt.title("Evolução diária do valor total de pedidos por grupo")
plt.xlabel("Data")
plt.ylabel("Valor total dos pedidos")
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Visualmente, as curvas estão extremamente parecidas o que não seria esperado em um teste A/B com grupos bem separados. Esse padrão costuma indicar interferência cruzada entre os grupos, mas já validamos que não há sobreposição de clientes entre eles.
# MAGIC
# MAGIC O grupo 01 (target) se mantém consistentemente acima do controle desde o início, e todas as oscilações sejam de alta ou baixa  ocorrem de forma sincronizada nos dois grupos, sugerindo um efeito forte de sazonalidade.

# COMMAND ----------

# noamalize
df_daily['orders_per_customer'] = df_daily['order_count'] / df_daily['unique_customers']
df_daily['amount_per_customer'] = df_daily['total_amount'] / df_daily['unique_customers']

plt.figure(figsize=(12, 5))
sns.lineplot(data=df_daily, x='date', y='orders_per_customer', hue='custumer_is_target', marker='o')
plt.title("Frequência de pedidos diária por cliente normalizado")
plt.ylabel("Pedidos por cliente")
plt.xlabel("Data")
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 5))
sns.lineplot(data=df_daily, x='date', y='amount_per_customer', hue='custumer_is_target', marker='o')
plt.title("Valor diário por cliente normalizado")
plt.ylabel("Valor total por cliente")
plt.xlabel("Data")
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()



# COMMAND ----------

# MAGIC %md
# MAGIC No entanto, parte da diferença observada pode ser explicada pela disparidade na base de clientes: o grupo tratado tem mais de 85 mil clientes a mais. Quando normalizamos as métricas diárias pelo número de clientes, vemos que a frequência de pedidos no grupo tratado fica levemente acima do controle a partir de janeiro. Isso pode indicar um efeito tardio do cupom, mas o impacto não aparece em todos os clientes, nem com grande intensidade.

# COMMAND ----------


df_daily['date'] = pd.to_datetime(df_daily['date'])


df_daily = df_daily.sort_values(by=['custumer_is_target', 'date'])


df_daily['order_count_cum'] = df_daily.groupby('custumer_is_target')['order_count'].cumsum()
df_daily['total_amount_cum'] = df_daily.groupby('custumer_is_target')['total_amount'].cumsum()


df_daily['unique_customers_cum'] = df_daily.groupby('custumer_is_target')['unique_customers'].cumsum()


df_daily['orders_per_customer_cum'] = df_daily['order_count_cum'] / df_daily['unique_customers_cum']
df_daily['amount_per_customer_cum'] = df_daily['total_amount_cum'] / df_daily['unique_customers_cum']




# COMMAND ----------

# MAGIC %md
# MAGIC ##### Graficos temporais acumulados

# COMMAND ----------

plt.figure(figsize=(12, 5))
sns.lineplot(data=df_daily, x='date', y='orders_per_customer_cum', hue='custumer_is_target', marker='o')
plt.title("Pedidos acumulados por cliente (normalizado)")
plt.ylabel("Pedidos por cliente (acumulado)")
plt.xlabel("Data")
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 5))
sns.lineplot(data=df_daily, x='date', y='amount_per_customer_cum', hue='custumer_is_target', marker='o')
plt.title("Valor acumulado por cliente (normalizado)")
plt.ylabel("Valor total por cliente (acumulado)")
plt.xlabel("Data")
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC O grupo 01 (target) apresenta um leve aumento na frequência de pedidos por cliente ao longo do tempo, especialmente após o início de janeiro, o que pode refletir um efeito cumulativo do cupom. Percebemos um maior ganho com os clientes target, mas como o valor acumulado por cliente se mantém praticamente igual entre os grupos, é provável que o estímulo tenha incentivado mais pedidoss com menor ticket médio.
# MAGIC
# MAGIC Isso pode indicar um comportamento racional de uso em que clientes do grupo 01 exploram o cupom para obter descontos em compras menores, o que aumenta o volume, mas não o gasto por cliente. Clientes ajustam seus pedidos para atingir o valor mínimo que aciona o cupom, mas evitam extrapolar, justamente para limitar o gasto próprio e maximizar o desconto relativo.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Diferença semanal entre os grupos em numeros de pedidos e valor normalizado

# COMMAND ----------

df_daily.head(5)

# COMMAND ----------

date_as_datetime = pd.to_datetime(df_daily["date"])
df_daily["week_date"] = date_as_datetime - pd.to_timedelta(date_as_datetime.dt.weekday, unit="D")

df_weekly = (
    df_daily
    .groupby(['week_date', 'custumer_is_target'])
    .agg(
        weekly_order_count=('order_count', 'sum'),
        weekly_total_amount=('total_amount', 'sum'),
        weekly_customers=('unique_customers', 'sum') #distinct client per week day
    )
    .reset_index()
)
# normalize columns
df_weekly['orders_per_customer'] = df_weekly['weekly_order_count'] / df_weekly['weekly_customers']
df_weekly['amount_per_customer'] = df_weekly['weekly_total_amount'] / df_weekly['weekly_customers']

# pivot
df_weekly_pivot = df_weekly.pivot(index='week_date', columns='custumer_is_target', values=[
    'orders_per_customer', 'amount_per_customer'])

df_weekly_pivot.columns = ['orders_per_customer_0', 'orders_per_customer_1',
                           'amount_per_customer_0', 'amount_per_customer_1']
df_weekly_pivot = df_weekly_pivot.reset_index()

# diff relative and absolut
df_weekly_pivot['diff_orders_per_customer'] = (
    df_weekly_pivot['orders_per_customer_1'] - df_weekly_pivot['orders_per_customer_0']
)
df_weekly_pivot['diff_amount_per_customer'] = (
    df_weekly_pivot['amount_per_customer_1'] - df_weekly_pivot['amount_per_customer_0']
)

plt.figure(figsize=(12, 5))
sns.lineplot(data=df_weekly_pivot, x='week_date', y='diff_orders_per_customer', marker='o')
plt.title("Diferença semanal de pedidos por cliente (Grupo 1 - Grupo 0)")
plt.ylabel("Pedidos por cliente (diferença)")
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 5))
sns.lineplot(data=df_weekly_pivot, x='week_date', y='diff_amount_per_customer', marker='o')
plt.title("Diferença semanal de valor por cliente (Grupo 1 - Grupo 0)")
plt.ylabel("Valor por cliente (diferença)")
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC A campanha gerou um leve aumento na frequência de pedidos por cliente, especialmente a partir de janeiro. A diferença semanal chegou a um pico de aproximadamente **+1 pedido adicional a cada 100 clientes**, mantendo-se depois em torno de **1 pedido a mais entre 142 a 200 clientes**.
# MAGIC
# MAGIC O valor por cliente permaneceu praticamente estável ao longo do período, com variações semanais entre **−R$ 0,80 e +R$ 0,50**, o que sugere um uso racional do cupom para pagar menos, sem aumento de gasto individual.
# MAGIC
# MAGIC O impacto, embora estatisticamente detectável, apresenta **baixa magnitude**, o que exige uma análise cuidadosa do custo do incentivo frente ao ganho incremental observado.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Avaliando ticket médio 
# MAGIC
# MAGIC - Uplift
# MAGIC - P-value

# COMMAND ----------

df_daily["average_ticket"] = df_daily["total_amount"] / df_daily["order_count"]

# COMMAND ----------

plt.figure(figsize=(12, 5))
sns.lineplot(data=df_daily, x='date', y='average_ticket', hue='custumer_is_target', marker='o')
plt.title("Evolução diária do ticket médio por grupo")
plt.xlabel("Data")
plt.ylabel("Ticket médio")
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()


# COMMAND ----------


ticket0 = order_metrics[order_metrics['custumer_is_target'] == 0]['customer_average_ticket']
ticket1 = order_metrics[order_metrics['custumer_is_target'] == 1]['customer_average_ticket']

# compare average ticket in groo
print(f'Ticket médio (controle): R${ticket0.mean():.2f}')
print(f'Ticket médio (target):  R${ticket1.mean():.2f}')
print(f'Uplift absoluto:          R${(ticket1.mean() - ticket0.mean()):.2f}')
print(f'Uplift percentual:       {(ticket1.mean() - ticket0.mean()) / ticket0.mean():.2%}')



# COMMAND ----------

# MAGIC %md 
# MAGIC A diferença do ticket médio reforma que os clientes do grupo target gastaram menos mesmo fazendo mais pedidos. Reforçando a hiporese que usaram o cupom para economizar no pedido, e não para gastar mais.
# MAGIC
# MAGIC O uplit absoluto nos diz quanto o grupo target gastou a mais ou a menos por pedido em média comparado ao grupo controle. Se o valor é positivo então o tratamento aumentou o resultado, se negativo reduziu, o que é o nosso caso. 
# MAGIC
# MAGIC O uplift percentual informa quanto em termos percentual a iniciativa alterou a metrica em relação ao grupo de controle. No nosso caso o ticket médio caiu no grupo target. 
# MAGIC
# MAGIC Portanto, os resultados sugerem que o uso do cupom motivou frequência, mas não aumento de receita por pedido, o que pode comprometer a rentabilidade caso o objetivo fosse elevar o ticket médio. É recomendável reavaliar o posicionamento estratégico da campanha, considerando ajustes no incentivo ou segmentação para alinhar o comportamento do cliente ao objetivo de crescimento em valor e não apenas em volume.

# COMMAND ----------

# using this beacause is assimetric
from scipy.stats import mannwhitneyu

stat, p = mannwhitneyu(ticket1, ticket0, alternative='two-sided')
print(f'p-valor: {p:.6f}')


# COMMAND ----------

# MAGIC %md 
# MAGIC O p-valor ≈ 0.00000 significa que a chance dessa diferença ter ocorrido por acaso é praticamente nula sob a hipótese nula de que as duas amostras são iguais. Importante informar que significância estatística não implica magnitude relevante.
# MAGIC
# MAGIC Isso tudo reforça a hipotese de que  a campanha gerou maior frequência de pedidos, mas reduziu ligeiramente o ticket médio.
# MAGIC
# MAGIC O ganho real depende de:
# MAGIC - quanto custou o cupom
# MAGIC - se o aumento no volume compensa a queda no ticket
# MAGIC - se houve engajamento de clientes novos ou reativação

# COMMAND ----------

# MAGIC %md
# MAGIC #### Avaliando tempo ativo (customer_lifetime_range)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Gráfico de barra utilizando desvio para calcular teste de confiança

# COMMAND ----------

order_metrics.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Avaliando tempo de vida do cliente na campanha
# MAGIC
# MAGIC Aqui avalio o tempo entre a primeira e ultimo pedido do cliente e trambém a media de tempo entre os pedidos

# COMMAND ----------


order_metrics["customer_lifetime_range"] = pd.to_numeric(order_metrics["customer_lifetime_range"], errors="coerce")
order_metrics["customer_lifetime_range_in_years"] = order_metrics["customer_lifetime_range"] / 365
order_metrics["custumer_is_target"] = order_metrics["custumer_is_target"].astype("category")


sns.set(style="whitegrid", palette="pastel", font_scale=1.1)


fig, axes = plt.subplots(1, 2, figsize=(14, 5))

metrics = ["customer_lifetime_range", "customer_lifetime_range_in_years"]
unidades = ["dias", "anos"]

for i, (metric, unidade) in enumerate(zip(metrics, unidades)):
    sns.histplot(
        data=order_metrics,
        x=metric,
        hue="custumer_is_target",
        element="step",
        stat="density",
        common_norm=False,
        bins=30,
        ax=axes[i]
    )
    axes[i].set_title(f"Distribuição de {metric.replace('_', ' ').title()} por grupo")
    axes[i].set_xlabel(f"Tempo de vida do cliente ({unidade})")
    axes[i].set_ylabel("Densidade")
    axes[i].grid(True, axis="y", linestyle="--", alpha=0.4)

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC A distribuição do tempo de vida dos clientes evidencia um padrão claro de concentração nos extremos. Observa-se que tanto o grupo controle quanto o grupo target apresentam picos nos primeiros dias de atividade e novamente em torno dos 30 dias, sugerindo uma taxa elevada de saída ou inatividade logo após o primeiro contato e outro comportamento recorrente ao redor de um mês de atividade.
# MAGIC
# MAGIC Contundo o grupo 1 (target) mostra-se ligeiramente mais presente nas faixas superiores de tempo de vida, o que sugere que esses clientes tendem a permanecer ativos por mais tempo na base.
# MAGIC
# MAGIC
# MAGIC  Esse comportamento pode indicar maior retenção ou fidelização, possivelmente influenciada pela ação promocional aplicada ao grupo.
# MAGIC

# COMMAND ----------


df = obt_coupon_analysis.copy()
df["order_date"] = pd.to_datetime(df["order_date"])

df = df.sort_values(["customer_id", "order_date"])


df["days_between_orders"] = df.groupby("customer_id")["order_date"].diff().dt.days


avg_interval = (
    df.groupby(["customer_id", "custumer_is_target"])["days_between_orders"]
    .mean()
    .reset_index()
    .rename(columns={"days_between_orders": "avg_days_between_orders"})
)

order_metrics = (
    order_metrics
    .drop(columns=["avg_days_between_orders"], errors="ignore")
    .merge(avg_interval, on=["customer_id", "custumer_is_target"], how="left")
)

# COMMAND ----------

sns.set(style="whitegrid", palette="pastel", font_scale=1.1)

plt.figure(figsize=(7, 5))
ax = sns.histplot(
    data=order_metrics,
    x="avg_days_between_orders",
    hue="custumer_is_target",
    element="step",
    stat="density",
    common_norm=False,
    bins=30
)

plt.title("Distribuição do tempo médio entre pedidos por grupo")
plt.xlabel("Dias entre pedidos (média por cliente)")
plt.ylabel("Densidade")
plt.grid(True, axis="y", linestyle="--", alpha=0.4)

#moving legend
ax.legend(title="Grupo", loc="upper left", bbox_to_anchor=(1, 1))

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Observamos que, embora a maioria dos clientes concentre seus pedidos com intervalos regulares próximos a 30 dias, o grupo 1 (target) apresenta uma densidade ligeiramente mais distribuída entre 5 e 15 dias, indicando uma frequência de pedidos mais curta.

# COMMAND ----------

def compare_ab_metric(df, metric, group_col="custumer_is_target", group_labels=(0, 1), label=None, unidade="dias"):
    """
    Compara uma métrica entre dois grupos utilizando média, uplift e teste de Mann-Whitney.
    
    Parâmetros:
        df (DataFrame): dataframe com os dados
        metric (str): nome da coluna da métrica a ser analisada
        group_col (str): coluna que define os grupos (padrão: 'custumer_is_target')
        group_labels (tuple): tupla com os dois valores dos grupos (padrão: (0, 1))
        label (str): nome legível da métrica (usado para impressão)
        unidade (str): unidade de medida, usada apenas para exibição
    """
    label = label or metric.replace("_", " ").title()

    group0 = df[df[group_col] == group_labels[0]][metric].dropna()
    group1 = df[df[group_col] == group_labels[1]][metric].dropna()

    mean0 = group0.mean()
    mean1 = group1.mean()
    uplift_abs = mean1 - mean0
    uplift_pct = uplift_abs / mean0 if mean0 != 0 else float("nan")

    print(f"### {label} ###")
    print(f"Média (grupo {group_labels[0]}): {mean0:.2f} {unidade}")
    print(f"Média (grupo {group_labels[1]}): {mean1:.2f} {unidade}")
    print(f"Uplift absoluto:  {uplift_abs:.2f} {unidade}")
    print(f"Uplift percentual: {uplift_pct:.2%}")

    stat, p = mannwhitneyu(group1, group0, alternative="two-sided")
    print(f"p-valor: {p:.6f}\n")


# COMMAND ----------

compare_ab_metric(order_metrics, "customer_lifetime_range", label="Customer Lifetime Range")
compare_ab_metric(order_metrics, "avg_days_between_orders", label="Avg Days Between Orders")

# COMMAND ----------

# MAGIC %md
# MAGIC **Tempo de vida do cliente:**
# MAGIC Grupo 1 apresentou um tempo médio de permanência 8,5% maior em comparação ao grupo controle (28,4 vs. 26,2 dias), com diferença estatisticamente significativa (p < 0,001).
# MAGIC
# MAGIC **Intervalo médio entre pedidos:** Os clientes do grupo 1 realizaram pedidos com 10,8% menos tempo entre compras (14,9 vs. 16,7 dias), também com significância estatística (p < 0,001).
# MAGIC
# MAGIC Essa evidência fortalece a hipótese de que a estratégia aplicada ao grupo 1 não apenas aumenta a longevidade do cliente na base, mas também eleva o nível de engajamento ao reduzir o intervalo entre compras. Isso é um indicativo positivo para ações promocionais com foco em recorrência de consumo.
# MAGIC O sistema de cupons pode gerar maior longevidade e engajamento com a plataforma.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Viabilidade Financeira da Estratégia de Cupons
# MAGIC Para avaliar a viabilidade financeira da campanha de cupons aplicada ao grupo target, propomos projetar o impacto financeiro da estratégia em escala, com base nos comportamentos médios observados no experimento realizado entre 03/12/2018 e 31/01/2019.
# MAGIC
# MAGIC A análise considera:
# MAGIC
# MAGIC - Receita gerada por cliente no período do teste, separada por grupo (controle e target).
# MAGIC - Média de pedidos por cliente em cada grupo.
# MAGIC - Projeção para uma base de 10.000 clientes, simulando um cenário de expansão da campanha.
# MAGIC - Custo promocional da campanha, assumindo que cada pedido no grupo target representa um cupom utilizado no valor de R$10.
# MAGIC
# MAGIC Margem bruta estimada de 35% sobre o valor da receita gerada.
# MAGIC
# MAGIC Comparação entre o lucro líquido estimado dos dois grupos, considerando o custo da campanha no grupo target e ausência de custo no grupo controle.

# COMMAND ----------

import numpy as np

summary = order_metrics.groupby("custumer_is_target").agg(
    total_revenue=("customer_monetary", "sum"),
    avg_orders_per_user=("customer_frequency", "mean"),
    n_customers=("customer_id", "nunique"),
    ticket_avg=('customer_average_ticket', 'mean')
).reset_index()

cupoum_discount = 0.1

summary["revenue_per_user"] = summary["total_revenue"] / summary["n_customers"]

revenue_per_user_0 = summary.loc[summary["custumer_is_target"] == 0, "revenue_per_user"].values[0]
revenue_per_user_1 = summary.loc[summary["custumer_is_target"] == 1, "revenue_per_user"].values[0]


diff = revenue_per_user_1 - revenue_per_user_0
percent = (diff / revenue_per_user_0) * 100

net_revenue_per_user_1 = revenue_per_user_1 * (1-cupoum_discount)

net_diff = net_revenue_per_user_1 - revenue_per_user_0
net_percent = (net_diff / revenue_per_user_0) * 100

print(f"Receita média por usuário (grupo 0): R${revenue_per_user_0:.2f}")
print(f"Receita média por usuário (grupo 1): R${revenue_per_user_1:.2f}")
print(f"Uplift absoluto (Bruto): R${diff:.2f}")
print(f"Uplift percentual (Bruto): {percent:.2f}%")
print(f"Receita liquida média por usuário (grupo 1): R${net_revenue_per_user_1:.2f}")
print(f"Uplift absoluto (Líquido): R${net_diff:.2f}")
print(f"Uplift percentual (Líquido): {net_percent:.2f}%")

# COMMAND ----------

summary.head()

# COMMAND ----------


colors = ["#aec7e8", "#98df8a"]  

fig, axs = plt.subplots(1, 2, figsize=(10, 4))


uplift_labels = ["Bruto", "Líquido"]
uplift_values_abs = [diff, net_diff]
uplift_values_pct = [percent, net_percent]


bars_abs = axs[0].bar(uplift_labels, uplift_values_abs, color=colors)
axs[0].set_title("Uplift Absoluto por Usuário (R$)", fontsize=12)
axs[0].set_ylabel("R$")
axs[0].grid(axis='y', linestyle='--', alpha=0.5)


for bar in bars_abs:
    height = bar.get_height()
    axs[0].text(bar.get_x() + bar.get_width()/2, height, f'R${height:.2f}',
                ha='center', va='bottom', fontsize=10)


bars_pct = axs[1].bar(uplift_labels, uplift_values_pct, color=colors)
axs[1].set_title("Uplift percentual por usuário (%)", fontsize=12)
axs[1].set_ylabel("%")
axs[1].grid(axis='y', linestyle='--', alpha=0.5)


for bar in bars_pct:
    height = bar.get_height()
    axs[1].text(bar.get_x() + bar.get_width()/2, height, f'{height:.2f}%',
                ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.show()



# COMMAND ----------

# MAGIC %pip install ace_tools

# COMMAND ----------

# parameters
base_users = 10000
month_base = net_diff * base_users  

# scenario definitions
scenarios = {
    "Pessimistic": 0.2,
    "Realistic": 0.5,
    "Optimistic": 0.8
}

# build monthly projection for each scenario (in millions)
projection_df = pd.DataFrame({"Month": np.arange(1, 13)})
annual_results = {}

for scenario_name, pct_active_users in scenarios.items():
    monthly_gain = month_base * pct_active_users / 1_000_000  
    cumulative_gain = np.cumsum([monthly_gain] * 12)
    projection_df[scenario_name] = cumulative_gain
    annual_results[scenario_name] = cumulative_gain[-1]  


plt.figure(figsize=(10, 6))
for scenario in scenarios:
    plt.plot(projection_df["Month"], projection_df[scenario], label=scenario)

plt.title("Projeçao de receita no ano considerando base de 10k clientes")
plt.xlabel("Mês")
plt.ylabel("Receita acumulada (R$ Milhões)")
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.1f}M'))
plt.legend(title="Cenários")
plt.grid(True, linestyle="--", alpha=0.6)
plt.tight_layout()
plt.show()

annual_df = pd.DataFrame.from_dict(annual_results, orient='index', columns=["Total Annual Revenue (R$ Milhões)"])
annual_df.index.name = "Scenario"
display(annual_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusão da análise financeira
# MAGIC
# MAGIC **Premissas da análise financeira utilizadas**
# MAGIC
# MAGIC A análise considera as seguintes premissas ajustadas com base na projeção financeira realizada:
# MAGIC
# MAGIC * **Período de avaliação:** 03/12/2018 até 31/01/2019.
# MAGIC * **Receita média por usuário:**
# MAGIC
# MAGIC   * **Grupo Controle (0):** R\$202,67
# MAGIC   * **Grupo Target (1):** R\$228,76 (bruto)
# MAGIC * **Valor médio do cupom (custo):** estimado em 10% de desconto aplicado sobre a receita do grupo target.
# MAGIC * **Receita líquida após cupom (grupo target):** R\$205,88 por usuário.
# MAGIC * **Margem operacional assumida:** implícita no valor líquido, já descontando o cupom promocional.
# MAGIC
# MAGIC **Resultados Financeiros Consolidados:**
# MAGIC
# MAGIC * **Uplift bruto:** R\$26,08 por usuário (12,87%)
# MAGIC * **Uplift líquido:** R\$3,21 por usuário (1,58%)
# MAGIC
# MAGIC **Avaliação Crítica Revisada**
# MAGIC
# MAGIC O resultado da campanha mostra que, após o desconto do cupom (10%), o ganho líquido real torna-se bastante limitado (apenas 1,58%). Embora exista um aumento significativo na receita bruta por usuário (12,87%), o desconto promocional reduz consideravelmente a rentabilidade líquida por usuário.
# MAGIC
# MAGIC Esse cenário revela que o estímulo atual é apenas marginalmente vantajoso. O cupom, embora eficaz em gerar maior frequência de compras, não resultou num aumento expressivo da rentabilidade líquida por cliente.
# MAGIC
# MAGIC **Considerações adicionais**
# MAGIC
# MAGIC É importante destacar que o ticket médio estável pode ser aceitável, desde que a frequência de compras aumente o suficiente para garantir um crescimento significativo na receita líquida acumulada ao longo do tempo. Portanto, uma métrica adicional recomendada para avaliação futura é a receita acumulada líquida por usuário, que capturará diretamente o impacto combinado da frequência e do ticket médio.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Proposta de campanha com foco também em aumento de ticket médio
# MAGIC
# MAGIC Para a próxima campanha, é fundamental adotar um modelo promocional que **estimule não apenas a frequência**, mas também o **aumento do ticket médio**, alinhando os incentivos aos nossos objetivos de **margem e rentabilidade**. Não é viável reter clientes cujo comportamento leva a prejuízo unitário, esse é um **trade-off insustentável** no longo prazo. O ideal é **direcionar o consumo**, incentivando carrinhos maiores e escolhas mais lucrativas.
# MAGIC
# MAGIC #### Recomendações
# MAGIC
# MAGIC * **Definir valor mínimo de compra mais elevado para uso do cupom:**
# MAGIC   Exemplo: 10% de desconto em compras superiores a R$$80, assegurando margens maiores após aplicação do cupom.
# MAGIC
# MAGIC * **Incentivar produtos ou categorias específicas com alta margem:**
# MAGIC   Exemplo: descontos direcionados exclusivamente para itens selecionados com margens acima da média.
# MAGIC
# MAGIC * **Condicionar benefícios adicionais (frete grátis ou cupons futuros):**
# MAGIC   Exemplo:
# MAGIC   * Frete grátis em compras acima de R$100
# MAGIC   * Cupom de R$10 para utilizar em próxima compara caso tenha consumido 100 reais em um pedido, dando uma ideia de cashback para usar quando quiser.
# MAGIC     Esses benefícios condicionais estimulam tanto o ticket atual quanto a recorrência rentável futura.
# MAGIC
# MAGIC #### Recomendação de validação do próximo teste
# MAGIC
# MAGIC Para validar essas novas hipóteses e avaliar o impacto real sobre rentabilidade, recomendo realizar um novo teste A/B com métricas claras de sucesso:
# MAGIC
# MAGIC * **Ticket médio líquido após desconto:** receita total líquida dividida pelo número de pedidos realizados após a aplicação dos descontos.
# MAGIC * **Frequência sustentável de compras:** média de pedidos por usuário ao longo do período analisado, indicando consistência no comportamento de compra.
# MAGIC * **Rentabilidade acumulada por usuário (LTV líquido):** soma da receita líquida gerada por cada usuário ao longo do período analisado.
# MAGIC * **Receita acumulada líquida por usuário:** receita total acumulada após descontos, dividida pelo número total de usuários ativos.
# MAGIC
# MAGIC Com esses ajustes, é possível maximizar o impacto positivo da campanha, garantindo que o crescimento em receita esteja alinhado à rentabilidade efetiva da operação.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

def gerar_projecao(ticket_base):
    df = order_metrics.copy()

    summary = df.groupby("custumer_is_target").agg(
        total_revenue=("customer_monetary", "sum"),
        total_orders=("customer_frequency", "sum"),
        n_customers=("customer_id", "nunique")
    ).reset_index()

    cupom_discount = 0.1

    #group 0
    summary["revenue_per_user"] = summary["total_revenue"] / summary["n_customers"]

    # group 1
    summary["simulated_revenue_per_user"] = (summary["total_orders"] * ticket_base) / summary["n_customers"]

    revenue_per_user_0 = summary.loc[summary["custumer_is_target"] == 0, "revenue_per_user"].values[0]
    revenue_per_user_1 = summary.loc[summary["custumer_is_target"] == 1, "simulated_revenue_per_user"].values[0]

    diff = revenue_per_user_1 - revenue_per_user_0
    percent = (diff / revenue_per_user_0) * 100

    net_revenue_per_user_1 = revenue_per_user_1 * (1 - cupom_discount)
    net_diff = net_revenue_per_user_1 - revenue_per_user_0
    net_percent = (net_diff / revenue_per_user_0) * 100

    print(f"Receita média por usuário (grupo 0): R${revenue_per_user_0:.2f}")
    print(f"Receita média simulada por usuário (grupo 1): R${revenue_per_user_1:.2f}")
    print(f"Uplift absoluto (Bruto): R${diff:.2f}")
    print(f"Uplift percentual (Bruto): {percent:.2f}%")
    print(f"Receita líquida média por usuário (grupo 1): R${net_revenue_per_user_1:.2f}")
    print(f"Uplift absoluto (Líquido): R${net_diff:.2f}")
    print(f"Uplift percentual (Líquido): {net_percent:.2f}%")

    # Projeção
    base_users = 100000
    month_base = net_diff * base_users

    scenarios = {
        "Pessimistic": 0.2,
        "Realistic": 0.5,
        "Optimistic": 0.8
    }

    projection_df = pd.DataFrame({"Month": np.arange(1, 13)})
    annual_results = {}

    for scenario_name, pct_active_users in scenarios.items():
        monthly_gain = month_base * pct_active_users / 1_000_000
        cumulative_gain = np.cumsum([monthly_gain] * 12)
        projection_df[scenario_name] = cumulative_gain
        annual_results[scenario_name] = cumulative_gain[-1]

    plt.figure(figsize=(10, 6))
    for scenario in scenarios:
        plt.plot(projection_df["Month"], projection_df[scenario], label=scenario)

    plt.title("Projeçao de receita no ano considerando base de 10k clientes")
    plt.xlabel("Mês")
    plt.ylabel("Receita acumulada (R$ Milhões)")
    plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.1f}M'))
    plt.legend(title="Cenários")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.show()

    annual_df = pd.DataFrame.from_dict(annual_results, orient='index', columns=["Total Annual Revenue (R$ Milhões)"])
    annual_df.index.name = "Scenario"
    display(annual_df)


gerar_projecao(80)

# COMMAND ----------

# MAGIC %md
# MAGIC #Salvando dados em .parquet na camada curated para utilizar 

# COMMAND ----------

from IPython.display import display

display(summary.head())
display(df_daily.head())
display(df_weekly.head())
display(obt_coupon_analysis.head())
display(order_metrics.head())


# COMMAND ----------

state = obt_coupon_analysis[['customer_id', 'order_state']].drop_duplicates(subset='customer_id')

dataframe_segmetation = pd.merge(order_metrics, state, on='customer_id', how='inner')

duplicated_cols = dataframe_segmetation.columns[dataframe_segmetation.columns.duplicated()]
print("Colunas duplicadas removidas:", list(duplicated_cols))

dataframe_segmetation_clean = dataframe_segmetation.loc[:, ~dataframe_segmetation.columns.duplicated()]

dataframe_segmetation_clean.to_parquet(f'{curated_dir}/dataframe_segmetation.parquet', index=False, engine="pyarrow")


# COMMAND ----------

for i in plt.get_fignums():
    fig = plt.figure(i)
    fig.savefig(f"grafico_{i}.png", dpi=300, bbox_inches='tight')