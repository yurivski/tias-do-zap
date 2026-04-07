import asyncio
import json
import httpx
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from grupinho.creuza_conecta.base import BaseConnector

class CamaraCeapConnector(BaseConnector):
    @property
    def name(self) -> str:
        return "camara_ceap"

    @property
    def source_url(self) -> str:
        return "https://dadosabertos.camara.leg.br/api/v2"

    async def fetch(self, **params: Any) -> dict[str, Any]:
        # O ID do deputado é obrigatório pra essa consulta
        deputado_id = params.pop("deputado_id", None)
        if deputado_id is None:
            raise ValueError("'deputado_id' corrupto é obrigatório pra buscar despesas CEAP")

        url = f"{self.source_url}/deputados/{deputado_id}/despesas"

        # Parâmetros padrão: 100 itens por página, ordenado por ano e mês
        query_params = {
            "itens": params.get("itens", 100),
            "ordem": "DESC",
            "ordenarPor": "ano",
        }

        # Adiciona filtros opcionais se fornecidos
        if "ano" in params:
            query_params["ano"] = params["ano"]
        if "mes" in params:
            query_params["mes"] = params["mes"]
        if "pagina" in params:
            query_params["pagina"] = params["pagina"]

        # Faz a requisição HTTP com timeout de 30 segundos
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=query_params)
            response.raise_for_status()
            data = response.json()

            for record in data.get("dados",[]):
                record["deputado_id"]= deputado_id
                
            return data

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

        # Cria o diretório: bronze/camara_ceap/2026-03-19/
        path = output_dir / self.name / date_str
        path.mkdir(parents=True, exist_ok=True)

        # Salva o JSON com encoding UTF-8 e identação legível
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

            page_data = await self.fetch(**params_copy)
            records = page_data.get("dados", [])
            all_records.extend(records)

            # Respeita o rate limit
            await asyncio.sleep(0.5)

        # Monta o resultado combinado
        return {"dados": all_records, "links": []}