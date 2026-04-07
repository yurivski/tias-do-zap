from typing import Any
from grupinho.creuza_conecta.transparencia_base import TransparenciaBaseConnector

class TransparenciaContratosConnector(TransparenciaBaseConnector):

    @property
    def name(self) -> str:
        return "transparencia_contratos"

    async def fetch(self, **params: Any) -> Any:
        # Código do órgão é obrigatório
        codigo_orgao = params.get("codigo_orgao")
        if not codigo_orgao:
            raise ValueError("'codigo_orgao' é obrigatório para buscar contratos")

        query_params = {
            "codigoOrgao": codigo_orgao,
            "pagina": params.get("pagina", 1),
        }

        # Filtros opcionais
        if "data_inicial" in params:
            query_params["dataInicial"] = params["data_inicial"]
        if "data_final" in params:
            query_params["dataFinal"] = params["data_final"]
        if "cnpj_fornecedor" in params:
            query_params["cnpjContratado"] = params["cnpj_fornecedor"]

        return await self._make_request("contratos", params=query_params)

    def validate(self, data: Any) -> bool:
        # A API de contratos retorna uma lista direto
        if not isinstance(data, list):
            return False

        return True