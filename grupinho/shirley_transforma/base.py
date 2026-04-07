import pandas as pd
import json
from abc import ABC, abstractmethod
from pathlib import Path

class BaseTransformer(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    @abstractmethod
    def source_connector(self) -> str:
        ...

    @abstractmethod
    def transform(self, bronze_dir: Path, silver_dir: Path) -> Path:
        ...
    
    def _read_bronze_files(self, bronze_dir: Path) -> list[dict]:
        # Caminho dos JSONs deste connector: bronze/{source_connector}/
        source_path = bronze_dir / self.source_connector
        all_records = []

        # Percorre todos os diretórios de data e arquivos JSON
        if not source_path.exists():
            return all_records

        for json_file in sorted(source_path.rglob("*.json")):
            raw = json.loads(json_file.read_text(encoding="utf-8"))

            # Extrai apenas a lista de dados, ignora links
            if "dados" in raw and isinstance(raw["dados"], list):
                all_records.extend(raw["dados"])

        return all_records
    
    def __repr__(self) -> str:
        # Representação legível pra logs e debug
        return f"<Transformer: {self.name}>"