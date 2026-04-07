from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from grupinho.marli_analisa.base import Alert, BaseAnalyzer


class SpendingSparkAnalyzer(BaseAnalyzer):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @property
    def name(self) -> str:
        return "spending"

    def analyze(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        alerts = []
        alerts.extend(self._check_supplier_concentration(gold_dir, deputado_id))
        alerts.extend(self._check_high_value_notes(gold_dir, deputado_id))
        return alerts

    def _check_supplier_concentration(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        fact = self.spark.read.format("delta").load(str(gold_dir / "fact_despesas_ceap"))
        dim_parlamentar = self.spark.read.format("delta").load(str(gold_dir / "dim_parlamentar"))
        dim_fornecedor = self.spark.read.format("delta").load(str(gold_dir / "dim_fornecedor"))

        if deputado_id is not None:
            keys = dim_parlamentar.filter(F.col("deputado_id") == deputado_id).select("parlamentar_key")
            fact = fact.join(keys, on="parlamentar_key", how="inner")

        gastos_por_fornecedor = (
            fact.groupBy("parlamentar_key", "fornecedor_key")
            .agg(F.sum("valor_liquido").alias("total_fornecedor"))
        )

        total_geral = (
            gastos_por_fornecedor.groupBy("parlamentar_key")
            .agg(F.sum("total_fornecedor").alias("total_geral"))
        )

        resultado = (
            gastos_por_fornecedor
            .join(total_geral, on="parlamentar_key")
            .withColumn("percentual", F.round(F.col("total_fornecedor") / F.col("total_geral") * 100, 2))
            .filter(F.col("total_fornecedor") / F.col("total_geral") > 0.40)
            .join(dim_parlamentar.select("parlamentar_key", "nome_parlamentar", "sigla_partido"), on="parlamentar_key")
            .join(dim_fornecedor.select("fornecedor_key", "nome_fornecedor", "cnpj_cpf_fornecedor"), on="fornecedor_key")
            .select("nome_parlamentar", "sigla_partido", "nome_fornecedor", "cnpj_cpf_fornecedor",
                    "total_fornecedor", "total_geral", "percentual")
            .orderBy(F.col("percentual").desc())
        )

        alerts = []
        for row in resultado.collect():
            alerts.append(Alert(
                code="SPD-001",
                description=f"Fornecedor concentra {row.percentual}% dos gastos do parlamentar",
                score=2,
                details={
                    "deputado": row.nome_parlamentar,
                    "partido": row.sigla_partido,
                    "fornecedor": row.nome_fornecedor,
                    "cnpj_cpf": row.cnpj_cpf_fornecedor,
                    "valor_fornecedor": round(row.total_fornecedor, 2),
                    "valor_total": round(row.total_geral, 2),
                    "percentual": row.percentual,
                },
            ))

        return alerts

    def _check_high_value_notes(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        """
        SPD-002: Detecta notas fiscais individuais com valor
        acima de 3 desvios padrão da média.
        """
        fact = self.spark.read.format("delta").load(str(gold_dir / "fact_despesas_ceap"))
        dim_parlamentar = self.spark.read.format("delta").load(str(gold_dir / "dim_parlamentar"))
        dim_fornecedor = self.spark.read.format("delta").load(str(gold_dir / "dim_fornecedor"))

        # Calcula média e desvio sobre TODOS os dados antes de filtrar pelo deputado.
        stats = fact.agg(
            F.avg("valor_liquido").alias("media"),
            F.stddev("valor_liquido").alias("desvio"),
        ).collect()[0]

        media, desvio = stats.media, stats.desvio

        if not desvio or desvio == 0:
            return []

        limite = media + (3 * desvio)

        if deputado_id is not None:
            keys = dim_parlamentar.filter(F.col("deputado_id") == deputado_id).select("parlamentar_key")
            fact = fact.join(keys, on="parlamentar_key", how="inner")

        resultado = (
            fact.filter(F.col("valor_liquido") > limite)
            .join(dim_fornecedor.select("fornecedor_key", "nome_fornecedor", "cnpj_cpf_fornecedor"), on="fornecedor_key")
            .select("cod_documento", "valor_liquido", "nome_fornecedor", "cnpj_cpf_fornecedor")
            .orderBy(F.col("valor_liquido").desc())
        )

        alerts = []
        for row in resultado.collect():
            alerts.append(Alert(
                code="SPD-002",
                description=f"Nota fiscal com valor R${row.valor_liquido:,.2f} acima de 3 desvios padrão (limite: R${limite:,.2f})",
                score=2,
                details={
                    "cod_documento": row.cod_documento,
                    "valor": round(row.valor_liquido, 2),
                    "fornecedor": row.nome_fornecedor,
                    "cnpj_cpf": row.cnpj_cpf_fornecedor,
                    "media": round(media, 2),
                    "desvio_padrao": round(desvio, 2),
                    "limite": round(limite, 2),
                },
            ))

        return alerts
