# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Pesquisa de Satisfação
# MAGIC
# MAGIC **Entrada:** `case.bronze.tb_pesquisa_satisfacao`
# MAGIC **Saídas (schema `case.silver`):**
# MAGIC * `tb_pesquisa_satisfacao` — tabela tratada, com flags de qualidade
# MAGIC * `tb_pesquisa_satisfacao_quarentena` — linhas retiradas da tabela principal (duplicatas e sem ID), nunca apagadas em silêncio
# MAGIC * `tb_pesquisa_satisfacao_qualidade` — métricas desta execução (insumo do relatório de confiabilidade)
# MAGIC
# MAGIC **Decisões de tratamento**
# MAGIC 1. Tudo é lido como texto e só então tipado (a Bronze usa `inferSchema`, então o tipo de entrada pode variar).
# MAGIC 2. Nota ausente **não é imputada**: fica nula e recebe `flag_sem_nota`. Imputar inventaria satisfação que não existe.
# MAGIC 3. Nota não numérica, não inteira ou fora de 1–5 vira nula e recebe flag; o valor original fica em `nota_original`.
# MAGIC 4. `id_pesquisa` duplicado: mantém a linha de ingestão mais recente e manda as demais para a quarentena.
# MAGIC 5. Mais de uma pesquisa para o mesmo `id_chamado`: **não remove**, apenas sinaliza (`qtd_pesquisas_chamado`, `flag_chamado_multiplas_pesquisas`). A regra de agregação fica para a Gold e para o log de decisões.
# MAGIC 6. **Idempotência:** a Silver é reconstruída por completo a partir da Bronze (`overwrite`). Reexecutar gera o mesmo conteúdo de negócio. O teste está na última célula.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------
# Parâmetros: ajustar só aqui
# ---------------------------------------------------------------
CATALOG = "case"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

TABELA_ORIGEM = "tb_pesquisa_satisfacao"
TABELA_DESTINO = "tb_pesquisa_satisfacao"
TABELA_QUARENTENA = "tb_pesquisa_satisfacao_quarentena"
TABELA_QUALIDADE = "tb_pesquisa_satisfacao_qualidade"

# Nome da Silver de chamados (confirmar com quem faz Chamados). Usado só na validação cruzada.
TABELA_CHAMADOS_SILVER = "tb_chamados"

COLUNAS_ESPERADAS = {"id_pesquisa", "id_chamado", "nota_atendimento"}
VALORES_NULOS = ["NULL", "NAN", "N/A", "NA", "NONE", "-", "NULO"]


def fq(schema, tabela):
    """Nome totalmente qualificado. Crases protegem o catálogo `case`, que é palavra reservada em SQL."""
    return f"`{CATALOG}`.{schema}.{tabela}"


spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{SILVER_SCHEMA}")
print(f"Origem : {fq(BRONZE_SCHEMA, TABELA_ORIGEM)}")
print(f"Destino: {fq(SILVER_SCHEMA, TABELA_DESTINO)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Perfilamento da Bronze (olhar antes de limpar)
# MAGIC Rode esta célula primeiro e leia o resultado. Se aparecer **uma coluna só** com tudo junto, o CSV usa `;` como separador e a Bronze precisa ser ajustada com `sep=";"`.

# COMMAND ----------

df_perfil = spark.table(fq(BRONZE_SCHEMA, TABELA_ORIGEM))
df_perfil.printSchema()
print("Linhas na Bronze:", df_perfil.count())

display(df_perfil.limit(20))

# Nulos por coluna
display(df_perfil.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df_perfil.columns]))

# Valores distintos da nota (procure 0, 6, negativos, decimais, texto)
col_nota = [c for c in df_perfil.columns if c.strip().lower() == "nota_atendimento"]
if col_nota:
    display(df_perfil.groupBy(col_nota[0]).count().orderBy(col_nota[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções de tratamento

# COMMAND ----------

def padronizar_colunas(df):
    """Nomes de coluna em minúsculas e sem espaços nas pontas."""
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower())
    return df


def texto_limpo(coluna):
    """Converte para texto, remove espaços e transforma vazio / 'NULL' / 'NaN' etc. em nulo."""
    c = F.trim(F.col(coluna).cast("string"))
    return F.when(
        (F.length(c) == 0) | F.upper(c).isin(VALORES_NULOS),
        F.lit(None).cast("string"),
    ).otherwise(c)


def tipar_e_sinalizar(df):
    df = padronizar_colunas(df)

    faltando = COLUNAS_ESPERADAS - set(df.columns)
    if faltando:
        raise ValueError(f"Colunas ausentes na Bronze: {faltando}. Encontradas: {df.columns}")

    df = (
        df.withColumn("id_pesquisa", texto_limpo("id_pesquisa"))
          .withColumn("id_chamado", texto_limpo("id_chamado"))
          .withColumn("nota_original", texto_limpo("nota_atendimento"))
          .withColumn("_nota_num", F.expr("try_cast(nota_original AS DOUBLE)"))
    )

    nao_inteira = F.col("_nota_num").isNotNull() & (F.col("_nota_num") != F.floor(F.col("_nota_num")))
    fora_dominio = F.col("_nota_num").isNotNull() & ((F.col("_nota_num") < 1) | (F.col("_nota_num") > 5))
    nota_valida = F.col("_nota_num").isNotNull() & ~nao_inteira & ~fora_dominio

    df = (
        df.withColumn("flag_sem_nota", F.col("nota_original").isNull())
          .withColumn("flag_nota_nao_numerica", F.col("nota_original").isNotNull() & F.col("_nota_num").isNull())
          .withColumn("flag_nota_nao_inteira", nao_inteira)
          .withColumn("flag_nota_fora_dominio", fora_dominio)
          .withColumn("flag_id_chamado_nulo", F.col("id_chamado").isNull())
          .withColumn("nota_atendimento", F.when(nota_valida, F.col("_nota_num").cast("int")))
          .drop("_nota_num")
    )

    if "ingestion_datetime" in df.columns:
        df = df.withColumnRenamed("ingestion_datetime", "bronze_ingestion_datetime")
    else:
        df = df.withColumn("bronze_ingestion_datetime", F.lit(None).cast("timestamp"))

    return df


def separar_quarentena(df):
    """Linhas sem id_pesquisa e duplicatas de id_pesquisa vão para a quarentena."""
    sem_chave = df.filter(F.col("id_pesquisa").isNull()).withColumn(
        "motivo_quarentena", F.lit("id_pesquisa_nulo")
    )
    com_chave = df.filter(F.col("id_pesquisa").isNotNull())

    w = Window.partitionBy("id_pesquisa").orderBy(
        F.col("bronze_ingestion_datetime").desc(),
        F.col("nota_original").desc_nulls_last(),
        F.col("id_chamado").desc_nulls_last(),
    )
    ranqueado = com_chave.withColumn("_rn", F.row_number().over(w))

    validos = ranqueado.filter("_rn = 1").drop("_rn")
    duplicados = (
        ranqueado.filter("_rn > 1").drop("_rn")
        .withColumn("motivo_quarentena", F.lit("duplicata_id_pesquisa"))
    )
    return validos, sem_chave.unionByName(duplicados)


def sinalizar_multiplas_pesquisas(df):
    w = Window.partitionBy("id_chamado")
    return (
        df.withColumn(
            "qtd_pesquisas_chamado",
            F.when(F.col("id_chamado").isNotNull(), F.count(F.lit(1)).over(w)),
        )
        .withColumn(
            "flag_chamado_multiplas_pesquisas",
            F.coalesce(F.col("qtd_pesquisas_chamado") > 1, F.lit(False)),
        )
    )


COLUNAS_FINAIS = [
    "id_pesquisa", "id_chamado", "nota_atendimento", "nota_original",
    "flag_sem_nota", "flag_nota_nao_numerica", "flag_nota_nao_inteira",
    "flag_nota_fora_dominio", "flag_id_chamado_nulo",
    "qtd_pesquisas_chamado", "flag_chamado_multiplas_pesquisas",
    "bronze_ingestion_datetime",
]


def gravar(df, schema, tabela):
    """Reconstrução completa da tabela: reexecutar não duplica nada."""
    (
        df.withColumn("silver_processed_datetime", F.current_timestamp())
          .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
          .saveAsTable(fq(schema, tabela))
    )


def calcular_qualidade(n_bronze, silver, quarentena):
    a = silver.agg(
        F.count(F.lit(1)).alias("linhas_silver"),
        F.sum(F.col("nota_atendimento").isNotNull().cast("int")).alias("notas_validas"),
        F.sum(F.col("flag_sem_nota").cast("int")).alias("sem_nota"),
        F.sum(F.col("flag_nota_nao_numerica").cast("int")).alias("nota_nao_numerica"),
        F.sum(F.col("flag_nota_nao_inteira").cast("int")).alias("nota_nao_inteira"),
        F.sum(F.col("flag_nota_fora_dominio").cast("int")).alias("nota_fora_dominio"),
        F.sum(F.col("flag_id_chamado_nulo").cast("int")).alias("id_chamado_nulo"),
        F.sum(F.col("flag_chamado_multiplas_pesquisas").cast("int")).alias("linhas_em_chamados_com_varias_pesquisas"),
    ).collect()[0].asDict()
    a = {k: float(v or 0) for k, v in a.items()}

    q = {r["motivo_quarentena"]: float(r["count"]) for r in quarentena.groupBy("motivo_quarentena").count().collect()}

    metricas = {"linhas_bronze": float(n_bronze), **a}
    metricas["quarentena_duplicata_id_pesquisa"] = q.get("duplicata_id_pesquisa", 0.0)
    metricas["quarentena_id_pesquisa_nulo"] = q.get("id_pesquisa_nulo", 0.0)
    metricas["pct_notas_validas"] = round(100 * a["notas_validas"] / a["linhas_silver"], 2) if a["linhas_silver"] else 0.0

    return spark.createDataFrame(list(metricas.items()), "metrica string, valor double")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execução (Bronze → Silver)

# COMMAND ----------

def executar():
    df_bronze = spark.table(fq(BRONZE_SCHEMA, TABELA_ORIGEM))
    n_bronze = df_bronze.count()

    df = tipar_e_sinalizar(df_bronze)
    validos, quarentena = separar_quarentena(df)
    silver = sinalizar_multiplas_pesquisas(validos).select(*COLUNAS_FINAIS)

    gravar(silver, SILVER_SCHEMA, TABELA_DESTINO)
    gravar(quarentena, SILVER_SCHEMA, TABELA_QUARENTENA)

    silver_tbl = spark.table(fq(SILVER_SCHEMA, TABELA_DESTINO))
    quarentena_tbl = spark.table(fq(SILVER_SCHEMA, TABELA_QUARENTENA))
    gravar(calcular_qualidade(n_bronze, silver_tbl, quarentena_tbl), SILVER_SCHEMA, TABELA_QUALIDADE)


executar()
display(spark.table(fq(SILVER_SCHEMA, TABELA_QUALIDADE)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Conferência dos resultados

# COMMAND ----------

silver_tbl = spark.table(fq(SILVER_SCHEMA, TABELA_DESTINO))

# Distribuição das notas (nulo = sem nota válida)
display(silver_tbl.groupBy("nota_atendimento").count().orderBy("nota_atendimento"))

# Amostra de linhas sinalizadas
display(
    silver_tbl.filter(
        "flag_sem_nota OR flag_nota_nao_numerica OR flag_nota_nao_inteira "
        "OR flag_nota_fora_dominio OR flag_chamado_multiplas_pesquisas"
    ).limit(30)
)

# Linhas que foram para a quarentena
display(spark.table(fq(SILVER_SCHEMA, TABELA_QUARENTENA)).limit(30))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação cruzada com Chamados (rodar no checkpoint de integração)
# MAGIC Só funciona depois que a Silver de Chamados existir. Mostra pesquisas órfãs e a taxa de resposta, que é o contexto necessário para interpretar o CSAT.

# COMMAND ----------

nome_chamados = fq(SILVER_SCHEMA, TABELA_CHAMADOS_SILVER)

if spark.catalog.tableExists(nome_chamados):
    chamados = (
        spark.table(nome_chamados)
        .select(F.trim(F.col("id_chamado").cast("string")).alias("id_chamado"))
        .distinct()
    )
    pesquisas = (
        spark.table(fq(SILVER_SCHEMA, TABELA_DESTINO))
        .filter("id_chamado IS NOT NULL")
        .select("id_chamado")
        .distinct()
    )
    orfas = pesquisas.join(chamados, "id_chamado", "left_anti").count()
    com_pesquisa = chamados.join(pesquisas, "id_chamado", "inner").count()
    total_chamados = chamados.count()

    print(f"Chamados (distintos)                  : {total_chamados}")
    print(f"Chamados com pelo menos uma pesquisa  : {com_pesquisa}")
    print(f"Taxa de resposta (chamados)           : {100 * com_pesquisa / total_chamados:.1f}%" if total_chamados else "sem chamados")
    print(f"id_chamado de pesquisa sem chamado    : {orfas}  (órfãs)")
else:
    print(f"{nome_chamados} ainda não existe. Rode esta célula depois que a Silver de Chamados estiver pronta.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Teste de idempotência
# MAGIC Executa o pipeline de novo e compara contagem e checksum do conteúdo de negócio (sem as colunas de auditoria). Os dois resultados precisam ser idênticos.

# COMMAND ----------

def checksum(tabela):
    df = spark.table(fq(SILVER_SCHEMA, tabela))
    cols = [c for c in df.columns if c not in ("bronze_ingestion_datetime", "silver_processed_datetime")]
    r = df.select(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("h"),
    ).collect()[0]
    return r["n"], r["h"]


antes = checksum(TABELA_DESTINO)
executar()
depois = checksum(TABELA_DESTINO)

print("Antes :", antes)
print("Depois:", depois)
assert antes == depois, "Pipeline NÃO é idempotente: o resultado mudou ao reexecutar."
print("OK: reexecutar produziu o mesmo resultado.")
