import duckdb
from pathlib import Path
from grupinho.marli_analisa.base import Alert, BaseAnalyzer

class SupplierAnalyzer(BaseAnalyzer):

    @property
    def name(self) -> str:
        return "supplier"

    def analyze(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:

        alerts = []
        alerts.extend(self._check_recent_supplier(gold_dir, deputado_id))
        alerts.extend(self._check_capital_incompatible(gold_dir, deputado_id))
        return alerts

    def _check_recent_supplier(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:

        alerts = []
        con = duckdb.connect()

        # Caminhos dos parquets na Silver
        silver_dir = gold_dir.parent / "silver"
        ceap_path = silver_dir / "ceap_despesas.parquet"
        empresas_path = silver_dir / "cnpj_empresas.parquet"

        # Verifica se os arquivos existem
        if not ceap_path.exists() or not empresas_path.exists():
            con.close()
            return alerts

        # Filtro opcional por deputado
        where_clause = ""
        if deputado_id is not None:
            where_clause = f"WHERE c.deputado_id = {deputado_id}"

        # Pra cada fornecedor, compara a data de abertura da empresa
        # com a data da primeira despesa que ela recebeu.
        # DATEDIFF conta a diferença em meses entre as duas datas.
        # Se a empresa foi criada menos de 12 meses antes da primeira
        # despesa, é suspeito — pode ter sido criada para receber
        # dinheiro público.
        # CPFs não aparecem em cnpj_empresas, então são excluídos
        # implicitamente pelo JOIN (sem match = sem linha no resultado).
        results = con.execute(f"""
            WITH primeira_despesa AS (
                SELECT
                    cnpj_cpf_fornecedor,
                    nome_fornecedor,
                    MIN(data_documento) AS primeira_data,
                    ROUND(SUM(valor_liquido), 2) AS total_recebido,
                    COUNT(*) AS qtd_notas
                FROM '{ceap_path}' c
                {where_clause}
                GROUP BY cnpj_cpf_fornecedor, nome_fornecedor
            )
            SELECT
                pd.cnpj_cpf_fornecedor,
                pd.nome_fornecedor,
                e.data_abertura,
                pd.primeira_data,
                DATEDIFF('month',
                    CAST(e.data_abertura AS DATE),
                    CAST(pd.primeira_data AS DATE)
                ) AS meses_diferenca,
                pd.total_recebido,
                pd.qtd_notas
            FROM primeira_despesa pd
            JOIN '{empresas_path}' e
                ON pd.cnpj_cpf_fornecedor = e.cnpj
            WHERE DATEDIFF('month',
                CAST(e.data_abertura AS DATE),
                CAST(pd.primeira_data AS DATE)
            ) < 12
            ORDER BY meses_diferenca ASC
        """).fetchall()

        for cnpj, nome, abertura, primeira, meses, total, qtd in results:
            alerts.append(Alert(
                code="SUP-001",
                description=f"Fornecedor criado {meses} meses antes da primeira despesa",
                score=2,
                details={
                    "fornecedor": nome,
                    "cnpj": cnpj,
                    "data_abertura": str(abertura),
                    "primeira_despesa": str(primeira),
                    "meses_diferenca": meses,
                    "total_recebido": total,
                    "qtd_notas": qtd,
                },
            ))

        con.close()
        return alerts

    def _check_capital_incompatible(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        """
        SUP-002: Detecta fornecedores cujo capital social é
        menor que o total recebido em despesas CEAP.
        """

        alerts = []
        con = duckdb.connect()

        silver_dir = gold_dir.parent / "silver"
        ceap_path = silver_dir / "ceap_despesas.parquet"
        empresas_path = silver_dir / "cnpj_empresas.parquet"

        if not ceap_path.exists() or not empresas_path.exists():
            con.close()
            return alerts

        # Filtro opcional por parlamentar
        where_clause = ""
        if deputado_id is not None:
            where_clause = f"WHERE c.deputado_id = {deputado_id}"

        # Compara o capital social declarado na Receita Federal
        # com o total que a empresa recebeu do parlamentar.
        # Se recebeu mais do que o capital social, é desproporcional.
        # Uma empresa com capital de R$10 mil recebendo R$63 mil
        # de um único deputado é anomalo.
        results = con.execute(f"""
            WITH gastos_por_fornecedor AS (
                SELECT
                    cnpj_cpf_fornecedor,
                    nome_fornecedor,
                    ROUND(SUM(valor_liquido), 2) AS total_recebido,
                    COUNT(*) AS qtd_notas
                FROM '{ceap_path}' c
                {where_clause}
                GROUP BY cnpj_cpf_fornecedor, nome_fornecedor
            )
            SELECT
                gf.cnpj_cpf_fornecedor,
                gf.nome_fornecedor,
                e.capital_social,
                gf.total_recebido,
                gf.qtd_notas,
                ROUND(gf.total_recebido / NULLIF(e.capital_social, 0) * 100, 2) AS percentual_do_capital
            FROM gastos_por_fornecedor gf
            JOIN '{empresas_path}' e
                ON gf.cnpj_cpf_fornecedor = e.cnpj
            WHERE e.capital_social > 0
                AND gf.total_recebido > e.capital_social
            ORDER BY percentual_do_capital DESC
        """).fetchall()

        for cnpj, nome, capital, total, qtd, pct in results:
            alerts.append(Alert(
                code="SUP-002",
                description=f"Fornecedor recebeu {pct}% do seu capital social",
                score=2,
                details={
                    "fornecedor": nome,
                    "cnpj": cnpj,
                    "capital_social": capital,
                    "total_recebido": total,
                    "qtd_notas": qtd,
                    "percentual_do_capital": pct,
                },
            ))

        con.close()
        return alerts
