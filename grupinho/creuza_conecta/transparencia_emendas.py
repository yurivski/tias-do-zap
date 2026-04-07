from typing import Any
from grupinho.creuza_conecta.transparencia_base import TransparenciaBaseConnector

class TransparenciaEmendasConnector(TransparenciaBaseConnector):

    @property
    def name(self) -> str:
        return "transparencia_emendas"

    async def fetch(self, **params: Any) -> Any:
        query_params = {"pagina": params.get("pagina", 1)}

        if "ano" in params:
            query_params["ano"] = params["ano"]
        if "nomeAutor" in params:
            query_params["nomeAutor"] = params["nomeAutor"]
        if "codigoEmenda" in params:
            query_params["codigoEmenda"] = params["codigoEmenda"]

        return await self._make_request("emendas", params=query_params)

    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False
        return True