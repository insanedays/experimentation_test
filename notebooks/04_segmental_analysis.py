# Databricks notebook source
# system and paths
import os
import sys
from pathlib import Path

# Viz
import matplotlib.pyplot as plt
import seaborn as sns

# data processing
import pandas as pd
from pyspark.sql import SparkSession


# load project paths
ROOT = Path.cwd().parent
sys.path.append(str(ROOT))

from utilis.config import get_paths

spark = SparkSession.builder.appName("ParquetReader").getOrCreate()

paths = get_paths()
temp_dir = paths["TMP_DIR"]
curated_dir = paths["CURATED_DIR"] 

# COMMAND ----------

dataframe_segmetation_path = f"{curated_dir}/dataframe_segmetation.parquet"

dataframe_segmetation = pd.read_parquet(dataframe_segmetation_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecionando clientes com maior potencial
# MAGIC
# MAGIC Selecionamos os 25% de clientes tratados com os maiores tickets médios, com o objetivo de identificar o perfil daqueles que apresentam maior potencial de valor para o negócio. Essa abordagem permite analisar comportamentos de compra mais qualificados e direcionar estratégias específicas de retenção ou expansão com base em características reais de alto desempenho.
# MAGIC

# COMMAND ----------

# 
df_filtered = dataframe_segmetation[dataframe_segmetation["custumer_is_target"] == 1].copy()

#  calculate 75th percentile (top 25%)
ticket_threshold = df_filtered["customer_average_ticket"].quantile(0.75)

#  threshold
top_25pct_clients = df_filtered[df_filtered["customer_average_ticket"] >= ticket_threshold]

print(f"Ticket médio mínimo para top 25%: R${ticket_threshold:.2f}")
print(f"Número de clientes selecionados: {len(top_25pct_clients)}")


# COMMAND ----------

top_25pct_clients.head()

# COMMAND ----------

top_25th_clients['customer_age'].describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segmentação baseada em impacto
# MAGIC
# MAGIC Realizada exclusivamente sobre o grupo 1 (target) com o objetivo de entender como a distribuição dos clientes se comportou entre todos os impactados e os que efetivamente performaram melhor (Top 25% de consumo).

# COMMAND ----------

# MAGIC %md
# MAGIC Em quais estados a campanha foi mais eficiente em atrair clientes de alto valor?"
# MAGIC
# MAGIC Para isso, comparamos por estado:
# MAGIC - A quantidade total de clientes impactados pela campanha (grupo target)
# MAGIC - A quantidade de clientes top 25% dentro desse mesmo grupo (com maior consumo)
# MAGIC
# MAGIC Avaliamos:
# MAGIC - Percentual de concentração de top 25% por estado
# MAGIC mede qual é a proporção de clientes valiosos em cada estado, em relação à própria base do estado. Esse percentual indica efetividade local da campanha: quanto mais alto, mais o estado respondeu com clientes de maior valor.
# MAGIC
# MAGIC -  Diferença percentual entre base completa e top 25%
# MAGIC Compara o peso de cada estado dentro da base total do target com seu peso dentro da base top 25%.
# MAGIC Responde: "Esse estado está super ou sub-representado entre os melhores clientes?"
# MAGIC
# MAGIC
# MAGIC Ao calcular a diferença percentual por estado entre a base total do target e o grupo dos melhores clientes, conseguimos visualizar onde a campanha teve maior capacidade de atrair clientes de alto valor, por outro lado, onde teve menor desempenho para conseguimos direcionar melhor a estratégia, priorizando as regiões que respondem bem ao estímulo.

# COMMAND ----------



# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

# 1. Calcular a distribuição percentual por estado
dist_25 = top_25pct_clients["order_state"].value_counts(normalize=True) * 100
dist_full = df_filtered["order_state"].value_counts(normalize=True) * 100

# 2. Unir os dois em um único DataFrame
dist_df = pd.concat([dist_25, dist_full], axis=1)
dist_df.columns = ["Top 25%", "Base completa"]

# 3. Preencher com 0 onde houver estado ausente em uma das bases
dist_df = dist_df.fillna(0)

# 4. Ordenar pela diferença percentual
diff_df = (dist_df["Top 25%"] - dist_df["Base completa"]).sort_values(ascending=False)

# 5. Plotar gráfico de barras ordenado
plt.figure(figsize=(12, 6))
bars = plt.bar(diff_df.index, diff_df.values)
plt.title("Diferença percentual de impacto por estado (Top 25% - Base completa)")
plt.xlabel("Estado")
plt.ylabel("Diferença percentual (%)")
plt.axhline(0, color="black", linewidth=0.8, linestyle="--")

# 6. Adicionar labels nas barras
for bar in bars:
    height = bar.get_height()
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        height + (1 if height >= 0 else -3),
        f"{height:.1f}%",
        ha='center',
        va='bottom' if height >= 0 else 'top'
    )

plt.tight_layout()
plt.show()



# COMMAND ----------

# MAGIC %md
# MAGIC Com base nos resultados, minha recomendação é que sigamos neste momento apenas com os estados de São Paulo (SP), Rio de Janeiro (RJ) e Bahia (BA), que mostraram maior concentração de clientes de alto valor em relação à base total do target.
# MAGIC
# MAGIC Esses estados demonstraram maior efetividade da campanha, superando expectativas já que alteraram sua distribuição padrão. Isso indica um perfil de cliente mais receptivo ao incentivo. A atuação focada neles permite maximizar o retorno sobre investimento e consolidar o modelo de retenção com mais segurança.
# MAGIC Quando a estratégia estiver validada e consolidada nesses mercados prioritários, poderemos avaliar a extensão para outros estados com ajustes específicos de abordagem, respeitando as diferenças de comportamento regional observadas.

# COMMAND ----------

top_counts = top_25pct_clients.groupby("order_state")["customer_id"].nunique().reset_index()
top_counts.columns = ["order_state", "top_25_count"]
total_counts = df_filtered.groupby("order_state")["customer_id"].nunique().reset_index()
total_counts.columns = ["order_state", "total_count"]

merged = pd.merge(total_counts, top_counts, on="order_state", how="left")
merged["top_25_count"] = merged["top_25_count"].fillna(0)
merged["pct_top_25"] = (merged["top_25_count"] / merged["total_count"]) * 100

import matplotlib.pyplot as plt


merged_sorted = merged.sort_values("pct_top_25", ascending=False)

plt.figure(figsize=(12, 6))
bars = plt.bar(merged_sorted["order_state"], merged_sorted["pct_top_25"])

plt.ylabel("% de clientes top 25% por estado")
plt.xlabel("Estado")
plt.title("Distribuição percentual de clientes top 25% por estado")
plt.xticks(rotation=45)

# Adiciona o texto em cada barra
for bar in bars:
    height = bar.get_height()
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        height,
        f'{height:.1f}%',
        ha='center',
        va='bottom',
        fontsize=9
    )

plt.tight_layout()
plt.show()

# COMMAND ----------

# Filtrar grupo target nos estados prioritários
prioritary_states = ["SP", "RJ", "BA"]
target_states_df = df_filtered[
    (df_filtered["custumer_is_target"] == 1) &
    (df_filtered["order_state"].isin(prioritary_states))
]

n_usuarios_prioritarios = target_states_df["customer_id"].nunique()


# Receita total líquida dos top 25%
top_liq_revenue = top_25pct_clients["customer_monetary"].sum()

# Número de clientes no top 25%
n_top_clients = top_25pct_clients["customer_id"].nunique()

# Receita líquida média por cliente top
avg_liq_per_top_user = top_liq_revenue / n_top_clients


# Projeção de receita bruta (sem custo do cupom ainda)
estimated_revenue = avg_liq_per_top_user * n_usuarios_prioritarios


# COMMAND ----------

import matplotlib.pyplot as plt

valores = [base_revenue_per_user, avg_liq_per_top_user, avg_liq_per_top_user * (1 - cupom_rate)]
labels = ['Receita base (antes)', 'Receita bruta (nova)', 'Receita líquida (nova)']

plt.figure(figsize=(8, 5))
bars = plt.bar(labels, valores, color=['gray', 'blue', 'green'])

for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 2, f'R${yval:.2f}', ha='center', fontsize=10)

plt.ylabel("Receita por cliente (R$)")
plt.title("Comparativo de receita por cliente – Base vs Estratégia com cupom")
plt.tight_layout()
plt.show()


# COMMAND ----------

59.9800

# COMMAND ----------

