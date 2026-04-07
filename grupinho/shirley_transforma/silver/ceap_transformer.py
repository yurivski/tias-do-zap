import pandas as pd
from pathlib import Path
from grupinho.shirley_transforma.base import BaseTransformer

class CeapTransformer(BaseTransformer):
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
        df = pd.DataFrame(records)

        # Limpeza e normalização
        # Renomeia colunas da API para o padrão do Argus (snake_case padronizado)
        df = df.rename(columns={
            "deputado_id": "deputado_id",
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
            "codLote": "cod_lote",
            "parcela": "parcela",
        })

        # Converte data_documento de string para datetime
        df["data_documento"] = pd.to_datetime(
            df["data_documento"],
            format="mixed",
            errors="coerce",
        )

        # Garante tipos de numéricos pra valores monetários
        df["valor_documento"] = pd.to_numeric(df["valor_documento"], errors="coerce")
        df["valor_liquido"] = pd.to_numeric(df["valor_liquido"], errors="coerce")
        df["valor_glosa"] = pd.to_numeric(df["valor_glosa"], errors="coerce")

        # Substitui strings vazias por None (viram null no Parquet)
        df = df.replace("", None)

        # Remove duplicatas pelo cod_documento (chave única da despesa)
        df = df.drop_duplicates(subset=["cod_documento"], keep="first")

        # Ordena por ano e mês pra consistência
        df = df.sort_values(
            by=["ano_referencia", "mes_referencia"],
            ascending=[True, True],
        ).reset_index(drop=True)

        # Persistência
        # Cria o diretório da silver se não existir
        silver_dir.mkdir(parents=True, exist_ok=True)

        # Salva como Parquet
        output_path = silver_dir / "ceap_despesas.parquet"
        df.to_parquet(output_path, index=False, engine="pyarrow")

        return output_path