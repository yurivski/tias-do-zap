import json
import httpx
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from grupinho.creuza_conecta.base import BaseConnector

class BrasilApiCNPJConnector(BaseConnector):
    @property
    def name(self) -> str:
        return "brasil_api_cnpj"

    @property
    def source_url(self) -> str:
        return "https://brasilapi.com.br/api/cnpj/v1"

    async def fetch(self, **params: Any) -> dict[str, Any]:
        cnpj = params.pop("cnpj", None)
        if cnpj is None:
            raise ValueError("'cnpj' é obrigatório para a detecção")

        url = f"{self.source_url}/{cnpj}"

        # Faz a requisição HTTP com timeout de 30 segundos
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            return data

    def validate(self, data: dict[str, Any]) -> bool:
        # Verifica se a chave 'cnpj' existe
        if "cnpj" not in data:
            return False

        return True

    def save_bronze(self, data: dict[str, Any], output_dir: Path) -> Path:
        # Gera o timestamp atual em UTC
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H%M%S")

        # Cria o diretório na bronze
        path = output_dir / self.name / date_str
        path.mkdir(parents=True, exist_ok=True)

        # Salva o JSON com encoding UTF-8 e identação legível
        file_path = path / f"{time_str}.json"
        file_path.write_text(
            json.dumps(data, 
            ensure_ascii=False, 
            indent=2),
            encoding="utf-8",
        )

        return file_path