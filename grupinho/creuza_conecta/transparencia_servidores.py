from typing import Any
from grupinho.creuza_conecta.transparencia_base import TransparenciaBaseConnector

class TransparenciaServidoresConnector(TransparenciaBaseConnector):

    @property
    def name(self) -> str:
        return "transparencia_servidores"

    async def fetch(self, **params: Any) -> Any:
        query_params = {"pagina": params.get("pagina", 1)}

        # Filtros opcionais
        if "cpf" in params:
            query_params["cpfServidor"] = params["cpf"]
        if "nome" in params:
            query_params["nome"] = params["nome"]
        if "orgao" in params:
            query_params["orgaoServidorExercicio"] = params["orgao"]

        return await self._make_request("servidores", params=query_params)

    def validate(self, data: Any) -> bool:
        # Retorna lista de servidores
        if not isinstance(data, list):
            return False
        return True
