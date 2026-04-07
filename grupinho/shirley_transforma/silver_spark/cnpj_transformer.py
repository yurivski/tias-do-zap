import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from grupinho.shirley_transforma.base import BaseTransformer

class CNPJSparkTransformer(BaseTransformer):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @property
    def name(self) -> str:
        return "cnpj"

    @property
    def source_connector(self) -> str:
        return "brasil_api_cnpj"

    def _read_bronze_files(self, bronze_dir: Path) -> list[dict]:
        # CNPJ: cada arquivo JSON é um registro individual de empresa (sem wrapper "dados")
        source_path = bronze_dir / self.source_connector
        all_records = []
        if not source_path.exists():
            return all_records
        for json_file in sorted(source_path.rglob("*.json")):
            raw = json.loads(json_file.read_text(encoding="utf-8"))
            all_records.append(raw)
        return all_records

    def transform(self, bronze_dir: Path, silver_dir: Path) -> Path:
        # Caminho dos JSONs na bronze
        all_records = self._read_bronze_files(bronze_dir)

        # antes de criar o DataFrame, padroniza o capital_social para float em todos os registros
        for record in all_records:
            if "capital_social" in record:
                record["capital_social"] = float(record["capital_social"]) if record["capital_social"] is not None else None
        
        if not all_records:
            raise ValueError("Nenhum registro encontrado na bronze para brasil_api_cnpj")

        # Extrai sócios (QSA) antes de criar o DataFrame principal
        socios_records = []
        for record in all_records:
            cnpj = record.get("cnpj", "")
            for socio in record.get("qsa") or []:
                socios_records.append({
                    "cnpj": cnpj,
                    "nome_socio": socio.get("nome_socio"),
                    "qualificacao_socio": socio.get("qualificacao_socio"),
                    "data_entrada_sociedade": socio.get("data_entrada_sociedade"),
                })

        # Mantém apenas campos escalares para evitar erro de inferência de schema
        columns_to_keep = [
            "cnpj", "razao_social", "nome_fantasia",
            "data_inicio_atividade", "capital_social",
            "descricao_situacao_cadastral", "cnae_fiscal_descricao",
            "uf", "municipio",
        ]
        flat_records = [
            {k: r.get(k) for k in columns_to_keep}
            for r in all_records
        ]

        df = self.spark.createDataFrame(flat_records)
        df = df.select([c for c in columns_to_keep if c in df.columns])

        renames = {
            "data_inicio_atividade": "data_abertura",
            "descricao_situacao_cadastral": "situacao_cadastral",
            "cnae_fiscal_descricao": "cnae_principal_descricao",
        }
        for old, new in renames.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)

        # Substitui strings vazias por null
        df = df.replace("", None)

        # Remove duplicatas pelo cnpj
        df = df.dropDuplicates(["cnpj"])

        # Ordena por nome_fantasia
        df = df.orderBy(col("nome_fantasia"))

        # Persistência
        # Salva como Delta
        output_path = silver_dir / "cnpj_empresas"
        df.write.format("delta").mode("overwrite").save(str(output_path))

        if socios_records:
            df_socios = self.spark.createDataFrame(socios_records)
            df_socios = df_socios.replace("", None)
            df_socios = df_socios.dropDuplicates()
            socios_path = silver_dir / "cnpj_socios"
            df_socios.write.format("delta").mode("overwrite").save(str(socios_path))

        return output_path
