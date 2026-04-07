from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from grupinho.marli_analisa.base import Alert, BaseAnalyzer


class SupplierSparkAnalyzer(BaseAnalyzer):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @property
    def name(self) -> str:
        return "supplier"

    def analyze(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        alerts = []
        alerts.extend(self._check_recent_supplier(gold_dir, deputado_id))
        alerts.extend(self._check_capital_incompatible(gold_dir, deputado_id))
        return alerts

    def _check_recent_supplier(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        # Lê as tabelas gold em formato Delta
        fact = self.spark.read.format("delta").load(str(gold_dir / "fact_despesas_ceap"))
        dim_parlamentar = self.spark.read.format("delta").load(str(gold_dir / "dim_parlamentar"))
        dim_fornecedor = self.spark.read.format("delta").load(str(gold_dir / "dim_fornecedor"))

        # cnpj_empresas ainda não tem tabela gold — lê direto da silver
        silver_dir = gold_dir.parent / "silver"
        dim_empresa = self.spark.read.format("delta").load(str(silver_dir / "cnpj_empresas"))

        # Filtra por deputado se especificado: resolve a parlamentar_key e restringe a fact
        if deputado_id is not None:
            keys = dim_parlamentar.filter(F.col("deputado_id") == deputado_id).select("parlamentar_key")
            fact = fact.join(keys, on="parlamentar_key", how="inner")

        # Pra cada fornecedor, encontra a primeira data em que ele recebeu uma despesa,
        # o total acumulado e a quantidade de notas fiscais emitidas
        primeira_despesa = (
            fact.groupBy("fornecedor_key")
            .agg(
                F.min("data_documento").alias("primeira_data"),       # data mais antiga da despesa
                F.round(F.sum("valor_liquido"), 2).alias("total_recebido"),
                F.count("*").alias("qtd_notas"),
            )
        )

        # Enriquece com CNPJ e nome do fornecedor — filtra apenas CNPJs (14+ chars), excluindo CPFs
        fornecedores = (
            primeira_despesa
            .join(
                dim_fornecedor.select("fornecedor_key", "cnpj_cpf_fornecedor", "nome_fornecedor"),
                on="fornecedor_key",
            )
            .filter(F.length(F.col("cnpj_cpf_fornecedor")) >= 14)
        )

        # Junta com os dados cadastrais da empresa para obter a data de abertura
        resultado = (
            fornecedores
            .join(
                dim_empresa.select("cnpj", "data_abertura"),
                on=fornecedores["cnpj_cpf_fornecedor"] == dim_empresa["cnpj"],
                how="inner",
            )
            # months_between(data_fim, data_inicio) retorna a diferença em meses como float;
            # floor converte para inteiro arredondando para baixo
            .withColumn(
                "meses_diferenca",
                F.floor(F.months_between(
                    F.col("primeira_data").cast("date"),
                    F.col("data_abertura").cast("date"),
                )).cast("int"),
            )
            # Empresas abertas menos de 12 meses antes da primeira despesa são suspeitas:
            # podem ter sido criadas especificamente para receber dinheiro público
            .filter(F.col("meses_diferenca") < 12)
            .orderBy(F.col("meses_diferenca").asc())
        )

        alerts = []
        for row in resultado.collect():
            alerts.append(Alert(
                code="SUP-001",
                description=f"Fornecedor criado {row.meses_diferenca} meses antes da primeira despesa",
                score=2,
                details={
                    "fornecedor": row.nome_fornecedor,
                    "cnpj": row.cnpj_cpf_fornecedor,
                    "data_abertura": str(row.data_abertura),
                    "primeira_despesa": str(row.primeira_data),
                    "meses_diferenca": row.meses_diferenca,
                    "total_recebido": row.total_recebido,
                    "qtd_notas": row.qtd_notas,
                },
            ))

        return alerts

    def _check_capital_incompatible(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        """
        SUP-002: Detecta fornecedores cujo capital social é
        menor que o total recebido em despesas CEAP.
        """
        # Lê as tabelas gold em formato Delta
        fact = self.spark.read.format("delta").load(str(gold_dir / "fact_despesas_ceap"))
        dim_parlamentar = self.spark.read.format("delta").load(str(gold_dir / "dim_parlamentar"))
        dim_fornecedor = self.spark.read.format("delta").load(str(gold_dir / "dim_fornecedor"))

        # cnpj_empresas ainda não tem tabela gold — lê direto da silver
        silver_dir = gold_dir.parent / "silver"
        dim_empresa = self.spark.read.format("delta").load(str(silver_dir / "cnpj_empresas"))

        # Filtra por deputado se especificado: resolve a parlamentar_key e restringe a fact
        if deputado_id is not None:
            keys = dim_parlamentar.filter(F.col("deputado_id") == deputado_id).select("parlamentar_key")
            fact = fact.join(keys, on="parlamentar_key", how="inner")

        # Agrega o total recebido por fornecedor e a quantidade de notas
        gastos_por_fornecedor = (
            fact.groupBy("fornecedor_key")
            .agg(
                F.round(F.sum("valor_liquido"), 2).alias("total_recebido"),
                F.count("*").alias("qtd_notas"),
            )
        )

        # Enriquece com CNPJ e nome do fornecedor — filtra apenas CNPJs (14+ chars), excluindo CPFs
        fornecedores = (
            gastos_por_fornecedor
            .join(
                dim_fornecedor.select("fornecedor_key", "cnpj_cpf_fornecedor", "nome_fornecedor"),
                on="fornecedor_key",
            )
            .filter(F.length(F.col("cnpj_cpf_fornecedor")) >= 14)
        )

        # Compara o capital social declarado na Receita Federal com o total recebido do parlamentar.
        # Uma empresa com capital de R$10k recebendo R$63k de um único deputado é anômalo.
        resultado = (
            fornecedores
            .join(
                dim_empresa.select("cnpj", "capital_social"),
                on=fornecedores["cnpj_cpf_fornecedor"] == dim_empresa["cnpj"],
                how="inner",
            )
            # Ignora empresas sem capital declarado e as que ainda não superaram o capital
            .filter((F.col("capital_social") > 0) & (F.col("total_recebido") > F.col("capital_social")))
            # Percentual do capital social que foi recebido via CEAP
            .withColumn(
                "percentual_do_capital",
                F.round(F.col("total_recebido") / F.col("capital_social") * 100, 2),
            )
            .orderBy(F.col("percentual_do_capital").desc())
        )

        alerts = []
        for row in resultado.collect():
            alerts.append(Alert(
                code="SUP-002",
                description=f"Fornecedor recebeu {row.percentual_do_capital}% do seu capital social",
                score=2,
                details={
                    "fornecedor": row.nome_fornecedor,
                    "cnpj": row.cnpj_cpf_fornecedor,
                    "capital_social": row.capital_social,
                    "total_recebido": row.total_recebido,
                    "qtd_notas": row.qtd_notas,
                    "percentual_do_capital": row.percentual_do_capital,
                },
            ))

        return alerts