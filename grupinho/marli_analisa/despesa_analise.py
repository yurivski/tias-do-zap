import duckdb
from pathlib import Path
from grupinho.marli_analisa.base import Alert, BaseAnalyzer


class SpendingAnalyzer(BaseAnalyzer):
    @property
    def name(self) -> str:
        return "spending"

    def analyze(self, gold_dir: Path, deputado_id) -> list[Alert]:
        alerts = []

        # Executa cada regra de detecção
        alerts.extend(self._check_supplier_concentration(gold_dir, deputado_id))
        alerts.extend(self._check_high_value_notes(gold_dir, deputado_id))

        return alerts

    def _check_supplier_concentration(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        alerts = []
        con = duckdb.connect()

        fact_path = gold_dir / "fact_despesas_ceap.parquet"
        dim_fornecedor = gold_dir / "dim_fornecedor.parquet"
        dim_parlamentar = gold_dir / "dim_parlamentar.parquet"

        # Filtro opcional por parlamentar
        where_clause = ""
        if deputado_id is not None:
            where_clause = f"WHERE dp.deputado_id = {deputado_id}"

        # Calcula a concentração de gastos por fornecedor pra cada parlamentar
        results = con.execute(f"""
            WITH gastos_por_fornecedor AS (
                SELECT
                    f.parlamentar_key,
                    f.fornecedor_key,
                    SUM(f.valor_liquido) AS total_fornecedor
                FROM '{fact_path}' f
                JOIN '{dim_parlamentar}' dp
                    ON f.parlamentar_key = dp.parlamentar_key {where_clause}
                GROUP BY f.parlamentar_key, f.fornecedor_key
            ),
            total_deputado AS (
                SELECT
                    parlamentar_key,
                    SUM(total_fornecedor) AS total_geral
                FROM gastos_por_fornecedor
                GROUP BY parlamentar_key
            )
            SELECT
                dp.nome_parlamentar,
                dp.sigla_partido,
                df.nome_fornecedor,
                df.cnpj_cpf_fornecedor,
                gf.total_fornecedor,
                td.total_geral,
                ROUND(gf.total_fornecedor / td.total_geral * 100, 2) AS percentual
            FROM gastos_por_fornecedor gf
            JOIN total_deputado td
                ON gf.parlamentar_key = td.parlamentar_key
            JOIN '{dim_parlamentar}' dp
                ON gf.parlamentar_key = dp.parlamentar_key
            JOIN '{dim_fornecedor}' df
                ON gf.fornecedor_key = df.fornecedor_key
            WHERE gf.total_fornecedor / td.total_geral > 0.40
            ORDER BY percentual DESC
        """).fetchall()

        for nome_dep, partido, nome_forn, cnpj, valor_forn, valor_total, pct in results:
            alerts.append(Alert(
                code="SPD-001",
                description=f"Fornecedor concentra {pct}% dos gastos do parlamentar",
                score=2,
                details={
                    "deputado": nome_dep,
                    "partido": partido,
                    "fornecedor": nome_forn,
                    "cnpj_cpf": cnpj,
                    "valor_fornecedor": round(valor_forn, 2),
                    "valor_total": round(valor_total, 2),
                    "percentual": pct,
                },
            ))

        con.close()
        return alerts

    def _check_high_value_notes(self, gold_dir: Path, deputado_id) -> list[Alert]:
        """
        SPD-002: Detecta notas fiscais individuais com valor
        acima de 3 desvios padrão da média.
        """

        alerts = []
        con = duckdb.connect()

        fact_path = gold_dir / "fact_despesas_ceap.parquet"
        dim_fornecedor = gold_dir / "dim_fornecedor.parquet"
        dim_parlamentar = gold_dir / "dim_parlamentar.parquet"

        # Filtro opcional por parlamentar
        where_clause = ""
        if deputado_id is not None:
            where_clause = f"""
                WHERE f.parlamentar_key IN (
                SELECT parlamentar_key FROM '{dim_parlamentar}'
                WHERE deputado_id = {deputado_id}
            )
        """

        # Calcula média e desvio padrão dos valores líquidos
        stats = con.execute(f"""
            SELECT
                AVG(valor_liquido) AS media,
                STDDEV(valor_liquido) AS desvio
            FROM '{fact_path}'
        """).fetchone()

        media, desvio = stats

        # Se desvio for zero ou null, não tem como calcular
        if not desvio or desvio == 0:
            con.close()
            return alerts

        # Limite: média + 3 desvios padrão
        limite = media + (3 * desvio)

        # Busca notas acima do limite
        results = con.execute(f"""
            SELECT
                f.cod_documento,
                f.valor_liquido,
                df.nome_fornecedor,
                df.cnpj_cpf_fornecedor
            FROM '{fact_path}' f
            JOIN '{dim_fornecedor}' df
                ON f.fornecedor_key = df.fornecedor_key
            {where_clause} AND f.valor_liquido > {limite}
            ORDER BY f.valor_liquido DESC
        """).fetchall()

        for cod_doc, valor, nome_forn, cnpj in results:
            alerts.append(Alert(
                code="SPD-002",
                description=f"Nota fiscal com valor R${valor:,.2f} acima de 3 desvios padrão (limite: R${limite:,.2f})",
                score=2,
                details={
                    "cod_documento": cod_doc,
                    "valor": round(valor, 2),
                    "fornecedor": nome_forn,
                    "cnpj_cpf": cnpj,
                    "media": round(media, 2),
                    "desvio_padrao": round(desvio, 2),
                    "limite": round(limite, 2),
                },
            ))

        con.close()
        return alerts