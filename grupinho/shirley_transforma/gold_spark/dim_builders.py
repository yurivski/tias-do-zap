from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, row_number


def build_dim_parlamentar(spark: SparkSession, silver_dir: Path, gold_dir: Path) -> Path:
    # Lê o delta de parlamentares da silver
    df = spark.read.format("delta").load(str(silver_dir / "deputados"))

    # Seleciona colunas para a dimensão e remove duplicatas pelo deputado_id
    dim = df.select(
        "deputado_id",
        "nome_parlamentar",
        "sigla_partido",
        "sigla_uf",
        "id_legislatura",
        "email"
    ).dropDuplicates(["deputado_id"])

    # Adiciona surrogate key monotonicamente crescente (não determinística entre execuções)
    dim = dim.withColumn("parlamentar_key", F.monotonically_increasing_id() + 1)

    # Reordena colunas com surrogate key na frente
    dim = dim.select(
        "parlamentar_key",
        "deputado_id",
        "nome_parlamentar",
        "sigla_partido",
        "sigla_uf",
        "id_legislatura",
        "email"
    )

    # Persiste na Gold como Delta
    output_path = gold_dir / "dim_parlamentar"
    dim.write.format("delta").mode("overwrite").save(str(output_path))

    return output_path

def build_dim_fornecedor(spark: SparkSession, silver_dir: Path, gold_dir: Path) -> Path:
    # Lê o delta de despesas da silver
    df = spark.read.format("delta").load(str(silver_dir / "ceap_despesas"))

    # Extrai fornecedores únicos
    dim = df.select(
        "cnpj_cpf_fornecedor",
        "nome_fornecedor"
    ).dropDuplicates(["cnpj_cpf_fornecedor"])

    # Adiciona surrogate key monotonicamente crescente (não determinística entre execuções)
    dim = dim.withColumn("fornecedor_key", F.monotonically_increasing_id() + 1)

    # Reordena colunas com surrogate key na frente
    dim = dim.select("fornecedor_key", "cnpj_cpf_fornecedor", "nome_fornecedor")

    # Persiste na Gold como Delta
    output_path = gold_dir / "dim_fornecedor"
    dim.write.format("delta").mode("overwrite").save(str(output_path))

    return output_path

def build_dim_categoria_despesa(spark: SparkSession, silver_dir: Path, gold_dir: Path) -> Path:
    # Lê o delta de despesas da silver
    df = spark.read.format("delta").load(str(silver_dir / "ceap_despesas"))

    # Extrai categorias únicas e renomeia para o padrão da dimensão
    dim = df.select(
        col("tipo_despesa").alias("descricao_despesa")
    ).dropDuplicates(["descricao_despesa"])

    # Adiciona surrogate key monotonicamente crescente (não determinística entre execuções)
    dim = dim.withColumn("categoria_key", F.monotonically_increasing_id() + 1)

    # Reordena colunas com surrogate key na frente
    dim = dim.select("categoria_key", "descricao_despesa")

    # Persiste na Gold como Delta
    output_path = gold_dir / "dim_categoria_despesa"
    dim.write.format("delta").mode("overwrite").save(str(output_path))

    return output_path
