import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from grupinho.shirley_transforma.base import BaseTransformer

class EmendasSparkTransformer(BaseTransformer):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @property
    def name(self) -> str:
        return "emendas"

    @property
    def source_connector(self) -> str:
        return "transparencia_emendas"

    def transform(self, bronze_dir: Path, silver_dir: Path) -> Path:
        source_path = bronze_dir / self.source_connector
        all_records = []

        if not source_path.exists():
            raise ValueError("Nenhum registro encontrado na bronze para transparencia_emendas")

        for json_file in sorted(source_path.rglob("*.json")):
            raw = json.loads(json_file.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                all_records.extend(raw)

        if not all_records:
            raise ValueError("Nenhum registro encontrado na bronze para transparencia_emendas")

        df = self.spark.createDataFrame(all_records)

        # Seleciona colunas relevantes
        columns_to_keep = [
            "codigoEmenda", "ano", "tipoEmenda", "autor",
            "nomeAutor", "numeroEmenda", "localidadeDoGasto",
            "funcao", "subfuncao", "valorEmpenhado",
            "valorLiquidado", "valorPago", "valorRestoInscrito",
            "valorRestoCancelado", "valorRestoPago",
        ]
        df = df.select([c for c in columns_to_keep if c in df.columns])

        # Renomeia para snake_case
        renames = {
            "codigoEmenda": "codigo_emenda",
            "tipoEmenda": "tipo_emenda",
            "nomeAutor": "nome_autor",
            "numeroEmenda": "numero_emenda",
            "localidadeDoGasto": "localidade_gasto",
            "valorEmpenhado": "valor_empenhado",
            "valorLiquidado": "valor_liquidado",
            "valorPago": "valor_pago",
            "valorRestoInscrito": "valor_resto_inscrito",
            "valorRestoCancelado": "valor_resto_cancelado",
            "valorRestoPago": "valor_resto_pago",
        }
        for old, new in renames.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)

        # Converte valores monetários (vêm como string "500.000,00")
        monetary_cols = [
            "valor_empenhado", "valor_liquidado", "valor_pago",
            "valor_resto_inscrito", "valor_resto_cancelado", "valor_resto_pago",
        ]
        for monetary_col in monetary_cols:
            if monetary_col in df.columns:
                df = df.withColumn(
                    monetary_col,
                    regexp_replace(col(monetary_col), r"\.", "")
                )
                df = df.withColumn(
                    monetary_col,
                    regexp_replace(col(monetary_col), ",", ".").cast("double")
                )

        # Substitui strings vazias por null
        df = df.replace("", None)

        # Remove duplicatas pelo codigo_emenda
        df = df.dropDuplicates(subset=["codigo_emenda"])

        # Ordena por ano e valor pago
        df = df.orderBy(col("ano"), col("valor_pago"))

        # Persistência
        output_path = str(silver_dir / "emendas")
        df.write.format("delta").mode("overwrite").save(output_path)

        return output_path
