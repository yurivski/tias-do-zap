import httpx
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from grupinho.creuza_conecta.base import BaseConnector
from grupinho.dulce_config.config import get_settings

class TransparenciaBaseConnector(BaseConnector):
    @property
    def source_url(self) -> str:
        settings = get_settings()
        return settings.transparencia_base_url

    def _get_headers(self) -> dict[str, str]:
        settings = get_settings()

        if not settings.transparencia_api_key:
            raise ValueError(
                "API KEY não configurada."
            )

        return{
            "chave-api-dados": settings.transparencia_api_key,
            "Accept": "application/json",
        }

    async def _make_request(self, endpoint: str, params: dict = None) -> Any:
        url = f"{self.source_url}/{endpoint}"
        headers = self._get_headers()

        # Requisição autenticada com timeout de 1 min
        # (Portal da Transparência pode ser lento)
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url, headers=headers, params=params or {})
            response.raise_for_status()
            return response.json()

    def save_bronze(self, data: dict[str, Any], output_dir: Path) -> Path:
        # Gera o timestamp atual em UTC
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H%M%S")

        # Cria o diretório na bronze
        path = output_dir / self.name / date_str
        path.mkdir(parents=True, exist_ok=True)

        # Salva o JSON
        file_path = path / f"{time_str}.json"
        file_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        return file_path