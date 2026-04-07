from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col


def build_fact_despesas_ceap(spark: SparkSession, silver_dir: Path, gold_dir: Path) -> Path:
    # Lê os dados da silver
    df = spark.read.format("delta").load(str(silver_dir / "ceap_despesas"))

    # Lê as dimensões da gold
    dim_parlamentar = spark.read.format("delta").load(str(gold_dir / "dim_parlamentar"))
    dim_fornecedor = spark.read.format("delta").load(str(gold_dir / "dim_fornecedor"))
    dim_categoria = spark.read.format("delta").load(str(gold_dir / "dim_categoria_despesa"))

    # JOIN com a dim_parlamentar, troca o "deputado_id" pela surrogate key "parlamentar_key"
    df = df.join(
        dim_parlamentar.select("parlamentar_key", "deputado_id"),
        on="deputado_id",
        how="left",
    )

    # JOIN com dim_fornecedor
    # Troca o cnpj_cpf_fornecedor pela surrogate key fornecedor_key
    df = df.join(
        dim_fornecedor.select("fornecedor_key", "cnpj_cpf_fornecedor"),
        on="cnpj_cpf_fornecedor",
        how="left",
    )

    # JOIN com dim_categoria_despesa
    # Troca o tipo_despesa pela surrogate key categoria_key
    df = df.join(
        dim_categoria.select("categoria_key", col("descricao_despesa").alias("tipo_despesa")),
        on="tipo_despesa",
        how="left",
    )

    # Monta a tabela fato final selecionando apenas as colunas relevantes
    fact = df.select(
        "parlamentar_key",
        "fornecedor_key",
        "categoria_key",
        "cod_documento",
        "num_documento",
        "data_documento",
        "ano_referencia",
        "mes_referencia",
        "valor_documento",
        "valor_liquido",
        "valor_glosa",
        "tipo_documento",
        "url_documento",
    )

    # Ordena por ano e mês
    fact = fact.orderBy("ano_referencia", "mes_referencia")

    # Adiciona surrogate key da fato
    fact = fact.withColumn("despesa_key", F.monotonically_increasing_id() + 1)

    # Reordena colunas com surrogate key na frente
    fact = fact.select(
        "despesa_key",
        "parlamentar_key",
        "fornecedor_key",
        "categoria_key",
        "cod_documento",
        "num_documento",
        "data_documento",
        "ano_referencia",
        "mes_referencia",
        "valor_documento",
        "valor_liquido",
        "valor_glosa",
        "tipo_documento",
        "url_documento",
    )

    # Persiste na gold como Delta
    output_path = gold_dir / "fact_despesas_ceap"
    fact.write.format("delta").mode("overwrite").save(str(output_path))

    return output_path
