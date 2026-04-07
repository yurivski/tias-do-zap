import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from grupinho.shirley_transforma.base import BaseTransformer


class DeputadosSparkTransformer(BaseTransformer):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @property
    def name(self) -> str:
        return "deputados"

    @property
    def source_connector(self) -> str:
        return "camara_deputados"

    def transform(self, bronze_dir: Path, silver_dir: Path) -> Path:
        records = self._read_bronze_files(bronze_dir)

        if not records:
            raise ValueError("Nenhum registro encontrado na Bronze para camara_deputados")

        df = self.spark.createDataFrame(records)

        # Renomeia colunas para o padrão snake_case do Argus
        renames = {
            "id": "deputado_id",
            "nome": "nome_parlamentar",
            "siglaPartido": "sigla_partido",
            "siglaUf": "sigla_uf",
            "idLegislatura": "id_legislatura",
            "urlFoto": "url_foto",
        }
        for old, new in renames.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)

        # Remove colunas desnecessárias
        cols_to_drop = [c for c in ("uri", "uriPartido") if c in df.columns]
        if cols_to_drop:
            df = df.drop(*cols_to_drop)

        # Substitui strings vazias por null
        df = df.replace("", None)

        # Remove duplicatas pelo id
        df = df.dropDuplicates(subset=["deputado_id"])

        # Ordena por nome e id para consistência
        df = df.orderBy(col("nome_parlamentar"), col("deputado_id"))

        output_path = silver_dir / "deputados"
        df.write.format("delta").mode("overwrite").save(str(output_path))

        return output_path
