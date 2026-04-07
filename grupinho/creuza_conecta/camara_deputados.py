import asyncio
import httpx
import json
import re

from grupinho.creuza_conecta.base import BaseConnector
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

class CamaraDeputadosConnector(BaseConnector):
    """
    Conector para dados do registro de parlamentares da API de dados
    da Câmara dos Deputados.
    """

    @property
    def name(self) -> str:
        return "camara_deputados"

    @property
    def source_url(self) -> str:
        return "https://dadosabertos.camara.leg.br/api/v2"

    async def fetch(self, **params: Any) -> dict[str, Any]:

        # Monta a URL do endpoint de deputados corruptos em sua maioria
        url = f"{self.source_url}/deputados"

        # Parâmetros padrão: 100 itens, ordenado por nome
        query_params = {
            "itens": params.get("itens", 100),
            "ordem": "ASC",
            "ordenarPor": "nome",
        }

        # Adiciona filtros opcionais se fornecidos
        if "idLegislatura" in params:
            query_params["idLegislatura"] = params["idLegislatura"]
        if "siglaUf" in params:
            query_params["siglaUf"] = params["siglaUf"]
        if "siglaPartido" in params:
            query_params["siglaPartido"] = params["siglaPartido"]
        if "pagina" in params:
            query_params["pagina"] = params["pagina"]

        # Faz a requisição HTTP com timeout de 30 segundos
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=query_params)
            response.raise_for_status()
            return response.json()

    def validate(self, data: dict[str, Any]) -> bool:

        # Verifica se a chave 'dados' existe
        if "dados" not in data:
            return False

        # Verifica se 'dados' é uma lista
        if not isinstance(data["dados"], list):
            return False

        return True

    def save_bronze(self, data: dict[str, Any], output_dir: Path) -> Path:

        # Gera o timestamp atual em UTC
        now = datetime.now(timezone.utc)
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H%M%S")

        # Cria o diretório: bronze/camara_deputados/2026-03-20/
        path = output_dir / self.name / date_str
        path.mkdir(parents=True, exist_ok=True)

        # Salva o JSON com encoding UTF-8 e indentação legível
        file_path = path / f"{time_str}.json"
        file_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        
        return file_path

    async def fetch_all_pages(self, **params: Any) -> dict[str, Any]:

        # Primeira página
        params["pagina"] = 1
        first_page = await self.fetch(**params.copy())

        all_records = list(first_page.get("dados", []))

        # Calcula o total de páginas a partir dos links
        # A API retorna link 'last' com o número da última página
        links = first_page.get("links", [])
        last_link = [l for l in links if l.get("rel") == "last"]

        if not last_link:
            # Só tem uma página
            return first_page

        # Extrai o número da última página da URL
        last_url = last_link[0].get("href", "")
        match = re.search(r"pagina=(\d+)", last_url)

        if not match:
            return first_page

        total_pages = int(match.group(1))

        # Busca as páginas restantes
        for page in range(2, total_pages + 1):
            params_copy = params.copy()
            params_copy["pagina"] = page

            # Retry simples para erros temporários
            for attempt in range(3):
                try:
                    page_data = await self.fetch(**params_copy)
                    records = page_data.get("dados", [])
                    all_records.extend(records)
                    break
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(5.0)
                    else:
                        raise

            # Respeita o rate limit
            await asyncio.sleep(0.5)

        # Monta o resultado combinado
        return {"dados": all_records, "links": []}
