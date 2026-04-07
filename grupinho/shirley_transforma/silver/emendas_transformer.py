import json
import pandas as pd
from pathlib import Path
from grupinho.shirley_transforma.base import BaseTransformer


class EmendasTransformer(BaseTransformer):

    @property
    def name(self) -> str:
        return "emendas"

    @property
    def source_connector(self) -> str:
        return "transparencia_emendas"

    def transform(self, bronze_dir: Path, silver_dir: Path) -> Path:
        # Leitura customizada (mesmo padrão do CNPJ - lista direto, não {"dados": [...]})
        source_path = bronze_dir / self.source_connector
        all_records = []

        if not source_path.exists():
            raise ValueError("Nenhum registro encontrado na bronze para transparencia_emendas")

        for json_file in sorted(source_path.rglob("*.json")):
            raw = json.loads(json_file.read_text(encoding="utf-8"))
            # A API retorna uma lista direto
            if isinstance(raw, list):
                all_records.extend(raw)

        if not all_records:
            raise ValueError("Nenhum registro encontrado na bronze para transparencia_emendas")

        df = pd.DataFrame(all_records)

        # Seleciona colunas relevantes
        columns_to_keep = [
            "codigoEmenda", "ano", "tipoEmenda", "autor",
            "nomeAutor", "numeroEmenda", "localidadeDoGasto",
            "funcao", "subfuncao", "valorEmpenhado",
            "valorLiquidado", "valorPago", "valorRestoInscrito",
            "valorRestoCancelado", "valorRestoPago",
        ]
        df = df[[c for c in columns_to_keep if c in df.columns]].copy()

        # Renomeia para snake_case
        df = df.rename(columns={
            "codigoEmenda": "codigo_emenda",
            "tipoEmenda": "tipo_emenda",
            "nomeAutor": "nome_autor",
            "numeroEmenda": "numero_emenda",
            "localidadeDoGasto": "localidade_gasto",
            "subfuncao": "subfuncao",
            "valorEmpenhado": "valor_empenhado",
            "valorLiquidado": "valor_liquidado",
            "valorPago": "valor_pago",
            "valorRestoInscrito": "valor_resto_inscrito",
            "valorRestoCancelado": "valor_resto_cancelado",
            "valorRestoPago": "valor_resto_pago",
        })

        # Converte valores monetários (vêm como string "500.000,00")
        monetary_cols = [
            "valor_empenhado", "valor_liquidado", "valor_pago",
            "valor_resto_inscrito", "valor_resto_cancelado", "valor_resto_pago",
        ]
        for col in monetary_cols:
            if col in df.columns:
                # Remove pontos de milhar e troca vírgula por ponto
                df[col] = df[col].astype(str).str.replace(".", "", regex=False)
                df[col] = df[col].str.replace(",", ".", regex=False)
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Substitui strings vazias por None
        df = df.replace("", None)

        # Remove duplicatas pelo codigo_emenda
        df = df.drop_duplicates(subset=["codigo_emenda"], keep="first")

        # Ordena por ano e valor pago
        df = df.sort_values(
            by=["ano", "valor_pago"],
            ascending=[True, False],
        ).reset_index(drop=True)

        # Persistência
        silver_dir.mkdir(parents=True, exist_ok=True)
        output_path = silver_dir / "emendas.parquet"
        df.to_parquet(output_path, index=False, engine="pyarrow")

        return output_path