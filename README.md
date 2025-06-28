# Análise do Teste A/B de Cupons – iFood

Este notebook documenta o processo completo de análise de um experimento A/B aplicado no iFood com o objetivo de avaliar o impacto da distribuição de cupons como estratégia de crescimento e retenção de clientes. O projeto inclui ingestão e tratamento dos dados, construção de métricas, análise estatística e financeira dos resultados, além de recomendações estratégicas com base em segmentações comportamentais.

A análise foi conduzida inteiramente dentro do ambiente **Databricks Community Edition**, utilizando PySpark, Pandas e bibliotecas auxiliares para visualização e testes estatísticos.

---

## Execução no Databricks

1. Importe os notebooks da pasta `/notebooks/` para seu workspace no Databricks.
2. Crie uma pasta `FileStore/raw/` no DBFS com os arquivos brutos (baixados automaticamente pelo notebook `01_ingestion_raw`).
3. Execute os notebooks na seguinte ordem:
   - `01_ingestion_raw`
   - `02_pre_processing`
   - `03_ab_test_analysis`
   - `04_segmetal_analysis`
4. As dependências estão listadas no arquivo `requirements.txt`.

> Obs.: Este projeto utiliza volumes do Unity Catalog. Certifique-se de possuir permissões de escrita em um catálogo ativo.

---

## Pré-requisitos

Este projeto foi estruturado com base em camadas de dados (`raw`, `refined`, `curated`) e utiliza configuração de caminhos via JSON.

Exemplo de `env.json`:

```json
{
  "CATALOG": "workspace",
  "SCHEMA": "db_uc",
  "RAW_VOLUME": "raw",
  "REFINED_VOLUME": "refined",
  "CURATED_VOLUME": "curated",
  "TMP_DIR": "/local_disk0/tmp/"
}
```

Veja instruções detalhadas em: [`data_catalog_config/README.md`](data_catalog_config/README.md)

---

## Questões em aberto

Durante a análise, algumas lacunas contextuais limitaram a profundidade das conclusões:

- Como funciona o processo de distribuição de cupons?
- O valor do cupom foi uniforme para todos os usuários?
- Todos os clientes do grupo tratamento efetivamente utilizaram os cupons?
- Qual foi o critério para a seleção da base de usuários no teste?

---

## Sugestões de variáveis adicionais

**Tempo de entrega real vs. prazo prometido**  
Permite avaliar se a logística influenciou a experiência do cliente. Atrasos podem impactar a percepção do serviço e a eficácia da campanha.

**Histórico de reclamações ou avaliações dos pedidos**  
Pode explicar parte da resposta negativa ou neutra à campanha. Permite segmentar clientes por grau de satisfação.

---

## Reprodutibilidade

- Todos os notebooks são versionados e podem ser exportados em PDF para entrega.
- Os caminhos de leitura e escrita estão centralizados em `config/config.json`.
- As métricas são calculadas a partir dos dados brutos conforme descrito nos notebooks.

---

## Apresentação Final

Este projeto inclui um material de apresentação com resumo executivo, principais achados e recomendações de negócio. O objetivo é facilitar a comunicação com stakeholders não técnicos.

📎 [Clique aqui para acessar a apresentação (adicionar link)](INSIRA_O_LINK_AQUI)

---

## Considerações finais

Apesar da campanha de cupons ter gerado aumento na frequência de pedidos por cliente, ela não elevou o ticket médio. O comportamento observado sugere que clientes do grupo tratamento usaram o cupom para economizar, fragmentando pedidos para obter mais descontos — o que resulta em maior volume com menor rentabilidade.

A análise de segmentação comportamental indicou que o cupom teve impacto heterogêneo. Clientes com maior ticket médio pré-existente responderam melhor. Já os clientes de baixo valor mostraram pouca resposta ou uso ineficiente do cupom. Campanhas futuras devem segmentar melhor o público-alvo e condicionar o uso do benefício a regras que garantam retorno financeiro (ex: valor mínimo de compra, incentivo em itens de alta margem, etc).

A análise sugere que a próxima campanha utilize:

- Métricas de ticket médio **líquido**, após o desconto
- Frequência de compras **sustentável** (com consistência no tempo)
- Receita acumulada **por usuário**, não apenas por pedido
- Estratégias geográficas focadas em estados com maior resposta observada

Com base nas evidências e projeções simuladas, uma reestruturação da política de cupons poderá maximizar o retorno e tornar a campanha financeiramente viável em escala.
