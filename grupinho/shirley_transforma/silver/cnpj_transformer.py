import pandas as pd
import json
from pathlib import Path
from grupinho.shirley_transforma.base import BaseTransformer

class CNPJTransformer(BaseTransformer):
    @property
    def name(self) -> str:
        return "cnpj"

    @property
    def source_connector(self) -> str:
        return "brasil_api_cnpj"

    def transform(self, bronze_dir: Path, silver_dir: Path) -> Path:
        # Caminho dos JSONs na bronze
        source_path = bronze_dir / self.source_connector
        all_records = []


        for json_file in sorted(source_path.rglob("*.json")):
            raw = json.loads(json_file.read_text(encoding="utf-8"))
            all_records.append(raw)

        if not all_records:
            raise ValueError("Nenhum registro encontrado na bronze para brasil_api_cnpj")
        
        df = pd.DataFrame(all_records)

        # Seleciona apenas as colunas relevantes
        columns_to_keep = [
            "cnpj", "razao_social", "nome_fantasia",
            "data_inicio_atividade", "capital_social",
            "descricao_situacao_cadastral", "cnae_fiscal_descricao",
            "uf", "municipio",
        ]
        df = df[columns_to_keep].copy()

        df = df.rename(columns={
            "cnpj": "cnpj",
            "razao_social": "razao_social",
            "nome_fantasia": "nome_fantasia",
            "data_inicio_atividade": "data_abertura",
            "capital_social": "capital_social",
            "descricao_situacao_cadastral": "situacao_cadastral",
            "cnae_fiscal_descricao": "cnae_principal_descricao",
            "uf": "uf",
            "municipio": "municipio"
        })

        df = df.replace("", None)

        # Remove duplicatas pelo cnpj
        df = df.drop_duplicates(subset=["cnpj"], keep="first")

        # Ordena por nome fantasia
        df = df.sort_values(
            by=["nome_fantasia"],
            ascending=[True],
        ).reset_index(drop=True)

        # Persistência
        # Cria o diretório da silver se não existir
        silver_dir.mkdir(parents=True, exist_ok=True)

        # Salva como Parquet
        output_path = silver_dir / "cnpj_empresas.parquet"
        df.to_parquet(output_path, index=False, engine="pyarrow")

        # Extrai sócios (QSA) de cada empresa
        socios_records = []
        for record in all_records:
            cnpj = record.get("cnpj", "")
            for socio in record.get("qsa", []):
                socios_records.append({
                    "cnpj": cnpj,
                    "nome_socio": socio.get("nome_socio"),
                    "qualificacao_socio": socio.get("qualificacao_socio"),
                    "data_entrada_sociedade": socio.get("data_entrada_sociedade"),
                })

        if socios_records:
            df_socios = pd.DataFrame(socios_records)
            df_socios = df_socios.replace("", None)
            df_socios = df_socios.drop_duplicates(keep="first")
            socios_path = silver_dir / "cnpj_socios.parquet"
            df_socios.to_parquet(socios_path, index=False, engine="pyarrow")
        
        return output_path