import asyncio
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from grupinho.marli_analisa.base import Alert, BaseAnalyzer
from grupinho.creuza_conecta.transparencia_ceis import TransparenciaCeisConnector
from grupinho.creuza_conecta.transparencia_cnep import TransparenciaCnepConnector

class SanctionsSparkAnalyzer(BaseAnalyzer):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @property
    def name(self) -> str:
        return "sanctions"

    def analyze(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        # Roda a verificação async dentro de um contexto síncrono
        return asyncio.run(self._check_sanctions(gold_dir, deputado_id))

    async def _check_sanctions(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        alerts = []

        # Lê as tabelas gold em formato Delta
        fact = self.spark.read.format("delta").load(str(gold_dir / "fact_despesas_ceap"))
        dim_parlamentar = self.spark.read.format("delta").load(str(gold_dir / "dim_parlamentar"))
        dim_fornecedor = self.spark.read.format("delta").load(str(gold_dir / "dim_fornecedor"))

        # Filtra por deputado se especificado
        if deputado_id is not None:
            keys = dim_parlamentar.filter(F.col("deputado_id") == deputado_id).select("parlamentar_key")
            fact = fact.join(keys, on="parlamentar_key", how="inner")

        # Agrega o total recebido por fornecedor e junta com a dimensão para obter CNPJ e nome
        fornecedores = (
            fact.groupBy("fornecedor_key")
            .agg(F.sum("valor_liquido").alias("total_recebido"))
            .join(
                dim_fornecedor.select("fornecedor_key", "nome_fornecedor", "cnpj_cpf_fornecedor"),
                on="fornecedor_key",
            )
            .filter(F.length(F.col("cnpj_cpf_fornecedor")) >= 14)  # Apenas CNPJs, não CPFs
            .select("cnpj_cpf_fornecedor", "nome_fornecedor", "total_recebido")
            .collect()
        )

        # Instancia os connectors
        ceis_conn = TransparenciaCeisConnector()
        cnep_conn = TransparenciaCnepConnector()

        for row in fornecedores:
            cnpj = row.cnpj_cpf_fornecedor
            nome_fornecedor = row.nome_fornecedor
            total_recebido = row.total_recebido

            # Consulta CEIS (empresas inidôneas)
            try:
                ceis_data = await ceis_conn.fetch(cnpj=cnpj)
                if ceis_conn.validate(ceis_data) and len(ceis_data) > 0:
                    for sancao in ceis_data:
                        cnpj_sancionado = sancao.get("sancionado", {}).get("codigoFormatado", "")
                        cnpj_sancionado = cnpj_sancionado.replace(".", "").replace("/", "").replace("-", "")
                        if cnpj_sancionado == cnpj:
                            alerts.append(Alert(
                                code="SUP-003",
                                description="Fornecedor consta no CEIS (empresas inidôneas)",
                                score=3,
                                details={
                                    "fornecedor": nome_fornecedor,
                                    "cnpj": cnpj,
                                    "total_recebido": round(total_recebido, 2),
                                    "tipo_sancao": sancao.get("tipoSancao", {}).get("descricaoResumida", ""),
                                    "data_inicio": sancao.get("dataInicioSancao", ""),
                                    "data_fim": sancao.get("dataFimSancao", ""),
                                    "orgao_sancionador": sancao.get("orgaoSancionador", {}).get("nome", ""),
                                },
                            ))
                            break
            except Exception:
                pass

            # Consulta CNEP (empresas punidas)
            try:
                cnep_data = await cnep_conn.fetch(cnpj=cnpj)
                if cnep_conn.validate(cnep_data) and len(cnep_data) > 0:
                    for sancao in cnep_data:
                        cnpj_sancionado = sancao.get("sancionado", {}).get("codigoFormatado", "")
                        cnpj_sancionado = cnpj_sancionado.replace(".", "").replace("/", "").replace("-", "")
                        if cnpj_sancionado == cnpj:
                            alerts.append(Alert(
                                code="SUP-004",
                                description="Fornecedor consta no CNEP (empresas punidas)",
                                score=3,
                                details={
                                    "fornecedor": nome_fornecedor,
                                    "cnpj": cnpj,
                                    "total_recebido": round(total_recebido, 2),
                                    "tipo_sancao": sancao.get("tipoSancao", {}).get("descricaoResumida", ""),
                                    "data_inicio": sancao.get("dataInicioSancao", ""),
                                    "data_fim": sancao.get("dataFimSancao", ""),
                                },
                            ))
                            break
            except Exception:
                pass

            # Respeita o rate limit entre requisições
            await asyncio.sleep(0.5)

        return alerts
