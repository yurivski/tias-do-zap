import pandas as pd
from pathlib import Path
from grupinho.shirley_transforma.base import BaseTransformer

class DeputadosTransformer(BaseTransformer):
    @property
    def name(self) -> str:
        return "deputados"

    @property
    def source_connector(self) -> str:
        return "camara_deputados"

    def transform(self, bronze_dir: Path, silver_dir: Path) -> Path:
        # Lê todos os registros da Bronze deste connector
        records = self._read_bronze_files(bronze_dir)

        if not records:
            raise ValueError("Nenhum registro encontrado na Bronze para camara_deputados")

        # Converte a lista de dicionários em DataFrame
        df = pd.DataFrame(records)

        # Limpeza e normalização
        # Renomeia colunas da API para o padrão do Argus (snake_case padronizado)
        df = df.rename(columns={
            "id": "deputado_id",
            "nome": "nome_parlamentar",
            "siglaPartido": "sigla_partido",
            "siglaUf": "sigla_uf",
            "idLegislatura": "id_legislatura",
            "urlFoto": "url_foto",
            "email": "email"
        })

        df = df.drop(columns=["uri", "uriPartido"], errors="ignore")

        # Substitui strings vazias por None (viram null no Parquet)
        df = df.replace("", None)

        # Remove duplicatas pelo id 
        df = df.drop_duplicates(subset=["deputado_id"], keep="first")

        # Ordena por nome_parlamentar e deputado_id pra consistência
        df = df.sort_values(
            by=["nome_parlamentar", "deputado_id"],
            ascending=[True, True],
        ).reset_index(drop=True)

        # Persistência
        # Cria o diretório da silver se não existir
        silver_dir.mkdir(parents=True, exist_ok=True)

        # Salva como Parquet
        output_path = silver_dir / "deputados.parquet"
        df.to_parquet(output_path, index=False, engine="pyarrow")

        return output_path