import asyncio
import pandas as pd
from pathlib import Path
from grupinho.marli_analisa.base import Alert, BaseAnalyzer
from grupinho.creuza_conecta.transparencia_ceis import TransparenciaCeisConnector
from grupinho.creuza_conecta.transparencia_cnep import TransparenciaCnepConnector


class SanctionsAnalyzer(BaseAnalyzer):
    @property
    def name(self) -> str:
        return "sanctions"

    def analyze(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:

        # Roda a verificação async dentro de um contexto sícrono
        return asyncio.run(self._check_sanctions(gold_dir, deputado_id))

    async def _check_sanctions(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        
        alerts = []

        # Lê os CNPJs dos fornecedores da silver
        silver_dir = gold_dir.parent / "silver"
        ceap_path = silver_dir / "ceap_despesas.parquet"

        if not ceap_path.exists():
            return alerts

        df = pd.read_parquet(ceap_path)

        # Filtra por deputados se especificado
        if deputado_id is not None:
            df = df[df["deputado_id"] == deputado_id]

        # Extrai CNPJs únicos (só CNPJs, não CPFs)
        cnpjs = df["cnpj_cpf_fornecedor"].dropna().unique().tolist()
        cnpjs = [c for c in cnpjs if len(str(c)) >= 14]

        # Instancia os connectors
        ceis_conn = TransparenciaCeisConnector()
        cnep_conn = TransparenciaCnepConnector()

        for cnpj in cnpjs:
            # Busca o nome do fornecedor para o relatório
            nome_fornecedor = df[df["cnpj_cpf_fornecedor"] == cnpj]["nome_fornecedor"].iloc[0]

            # Calcula o total recebido por esse fornecedor
            total_recebido = df[df["cnpj_cpf_fornecedor"] == cnpj]["valor_liquido"].sum()

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
