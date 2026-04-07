import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from grupinho.shirley_transforma.base import BaseTransformer

class CeapSparkTransformer(BaseTransformer):
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    @property
    def name(self) -> str:
        return "ceap"

    @property
    def source_connector(self) -> str:
        return "camara_ceap"

    def transform(self, bronze_dir: Path, silver_dir: Path) -> Path:
        # Lê todos os registros da Bronze deste connector
        records = self._read_bronze_files(bronze_dir)

        if not records:
            raise ValueError("Nenhum registro encontrado na Bronze para camara_ceap")

        # Converte a lista de dicionários em DataFrame
        df = self.spark.createDataFrame(records)

        # Limpeza e normalização
        # Renomeia colunas da API para o padrão do Argus (snake_case padronizado)
        renames = {
            "ano": "ano_referencia",
            "mes": "mes_referencia",
            "tipoDespesa": "tipo_despesa",
            "codDocumento": "cod_documento",
            "tipoDocumento": "tipo_documento",
            "codTipoDocumento": "cod_tipo_documento",
            "dataDocumento": "data_documento",
            "numDocumento": "num_documento",
            "valorDocumento": "valor_documento",
            "urlDocumento": "url_documento",
            "nomeFornecedor": "nome_fornecedor",
            "cnpjCpfFornecedor": "cnpj_cpf_fornecedor",
            "valorLiquido": "valor_liquido",
            "valorGlosa": "valor_glosa",
            "numRessarcimento": "num_ressarcimento",
            "codLote": "cod_lote"
        }
        for old, new in renames.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)

        # Converte data_documento de string para datetime
        df = df.withColumn(
            "data_documento",
            to_date(col("data_documento"))
        )

        # Garante tipos de numéricos pra valores monetários
        df = df.withColumn("valor_documento", col("valor_documento").cast("double")) \
                .withColumn("valor_liquido", col("valor_liquido").cast("double")) \
                .withColumn("valor_glosa", col("valor_glosa").cast("double"))

        # Substitui strings vazias por None (viram null no spark)
        df = df.replace("", None)

        # Remove duplicatas pelo cod_documento (chave única da despesa)
        df = df.dropDuplicates(subset=["cod_documento"])

        # Ordena por ano e mês pra consistência
        df = df.orderBy(col("ano_referencia"), col("mes_referencia"))

        # Persistência
        # Salva como delta
        output_path = str(silver_dir / "ceap_despesas")
        df.write.format("delta").mode("overwrite").save(str(output_path))

        return output_path