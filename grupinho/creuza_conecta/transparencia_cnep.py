from typing import Any
from grupinho.creuza_conecta.transparencia_base import TransparenciaBaseConnector

class TransparenciaCnepConnector(TransparenciaBaseConnector):

    @property
    def name(self) -> str:
        return "transparencia_cnep"

    async def fetch(self, **params: Any) -> Any:
        query_params = {"pagina": params.get("pagina", 1)}

        if "cnpj" in params:
            query_params["cnpjSancionado"] = params["cnpj"]
        if "nome" in params:
            query_params["nomeSancionado"] = params["nome"]

        return await self._make_request("cnep", params=query_params)

    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False
        return True