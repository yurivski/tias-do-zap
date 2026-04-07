<div align="center">

# APIs e Endpoints

APIs públicas consumidas pelas Tias do Zap e os endpoints específicos em uso.

</div>

---

## Sumário

- [Câmara dos Deputados](#câmara-dos-deputados)
- [BrasilAPI](#brasilapi)
- [Portal da Transparência](#portal-da-transparência)

---

## Câmara dos Deputados

Base URL: `https://dadosabertos.camara.leg.br/api/v2`

Autenticação: nenhuma.

Documentação oficial: [dadosabertos.camara.leg.br/swagger/api.html](https://dadosabertos.camara.leg.br/swagger/api.html)

### Endpoints em uso

<table>
<thead>
<tr><th>Endpoint</th><th>Método</th><th>Connector</th><th>Parâmetros usados</th><th>O que retorna</th></tr>
</thead>
<tbody>
<tr>
  <td><code>/deputados</code></td>
  <td>GET</td>
  <td>CamaraDeputadosConnector</td>
  <td><code>idLegislatura</code>, <code>pagina</code>, <code>itens</code> (obrigatórios) · <code>siglaUf</code>, <code>siglaPartido</code> (opcionais)</td>
  <td>Lista de deputados com id, nome, siglaPartido, siglaUf</td>
</tr>
<tr>
  <td><code>/deputados/{id}/despesas</code></td>
  <td>GET</td>
  <td>CamaraCeapConnector</td>
  <td><code>pagina</code>, <code>itens</code>, <code>ordem</code>, <code>ordenarPor</code> · <code>ano</code>, <code>mes</code> (opcionais)</td>
  <td>Despesas CEAP do deputado com valor, fornecedor, CNPJ, tipo</td>
</tr>
</tbody>
</table>

### Paginação

Ambos os endpoints usam paginação automática via `fetch_all_pages`:

1. Busca a primeira página com `pagina=1`
2. Lê o campo `links` da resposta e localiza o objeto com `rel == "last"`
3. Extrai o número da última página via regex (`pagina=(\d+)` na URL do link)
4. Itera de `pagina=2` até a última, com delay de **0.5s** entre páginas

A resposta segue o padrão `{"dados": [...], "links": [...]}`. Quando há apenas uma página, o campo `links` não contém o link `last` e o connector retorna os dados imediatamente.

O `CamaraDeputadosConnector` adiciona **retry automático de 3 tentativas** com 5s de espera entre elas para erros temporários durante a paginação.

### Injeção de deputado_id

A API `/deputados/{id}/despesas` não inclui o `deputado_id` em cada registro de despesa. O `CamaraCeapConnector` injeta esse campo manualmente antes de salvar na Bronze:

```python
for record in data.get("dados", []):
    record["deputado_id"] = deputado_id
```

Isso é necessário para que os transformers consigam fazer o JOIN entre despesas e parlamentares.

### Timeout e validação

| Connector | Timeout | Validação |
|---|---|---|
| `CamaraDeputadosConnector` | 30s | `"dados"` existe e é lista |
| `CamaraCeapConnector` | 30s | `"dados"` existe e é lista |

---

## BrasilAPI

Base URL: `https://brasilapi.com.br/api/cnpj/v1`

Autenticação: nenhuma.

Documentação oficial: [brasilapi.com.br/docs](https://brasilapi.com.br/docs)

### Endpoints em uso

<table>
<thead>
<tr><th>Endpoint</th><th>Método</th><th>Connector</th><th>Parâmetros usados</th><th>O que retorna</th></tr>
</thead>
<tbody>
<tr>
  <td><code>/cnpj/v1/{cnpj}</code></td>
  <td>GET</td>
  <td>BrasilApiCNPJConnector</td>
  <td>CNPJ na URL (14 dígitos, sem formatação)</td>
  <td>Dados da empresa: razão social, capital social, data de abertura, sócios (QSA), CNAE, endereço</td>
</tr>
</tbody>
</table>

A resposta é o objeto da empresa diretamente (não vem dentro de `{"dados": [...]}`). O campo `qsa` é uma lista de sócios com nome, qualificação e data de entrada na sociedade.

Rate limit: não documentado oficialmente, mas requisições excessivas retornam HTTP 429. A Dona Zilda usa delay de **1.0–1.5 segundos** entre requisições no pipeline de enriquecimento.

### Timeout e validação

| Connector | Timeout | Validação |
|---|---|---|
| `BrasilApiCNPJConnector` | 30s | campo `"cnpj"` presente no objeto retornado |

---

## Portal da Transparência

Base URL: `https://api.portaldatransparencia.gov.br/api-de-dados`

Autenticação: header `chave-api-dados` com token obtido em [portaldatransparencia.gov.br/api-de-dados/cadastrar-email](https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email).

Documentação oficial: [api.portaldatransparencia.gov.br/swagger-ui.html](https://api.portaldatransparencia.gov.br/swagger-ui.html)

### Endpoints em uso

<table>
<thead>
<tr><th>Endpoint</th><th>Método</th><th>Connector</th><th>Parâmetros usados</th><th>O que retorna</th></tr>
</thead>
<tbody>
<tr>
  <td><code>/ceis</code></td>
  <td>GET</td>
  <td>TransparenciaCeisConnector</td>
  <td><code>cnpjSancionado</code>, <code>pagina</code> · <code>nomeSancionado</code> (opcional)</td>
  <td>Empresas inidôneas e suspensas (CEIS): sanção, órgão sancionador, datas</td>
</tr>
<tr>
  <td><code>/cnep</code></td>
  <td>GET</td>
  <td>TransparenciaCnepConnector</td>
  <td><code>cnpjSancionado</code>, <code>pagina</code> · <code>nomeSancionado</code> (opcional)</td>
  <td>Empresas punidas (CNEP): tipo de sanção, datas, fundamentação</td>
</tr>
<tr>
  <td><code>/emendas</code></td>
  <td>GET</td>
  <td>TransparenciaEmendasConnector</td>
  <td><code>nomeAutor</code>, <code>pagina</code> · <code>ano</code>, <code>codigoEmenda</code> (opcionais)</td>
  <td>Emendas parlamentares: localidade, função, valores empenhados/pagos</td>
</tr>
<tr>
  <td><code>/contratos</code></td>
  <td>GET</td>
  <td>TransparenciaContratosConnector</td>
  <td><code>codigoOrgao</code> (obrigatório), <code>pagina</code> · <code>dataInicial</code>, <code>dataFinal</code>, <code>cnpjContratado</code> (opcionais)</td>
  <td>Contratos federais: número, objeto, fornecedor, valor</td>
</tr>
<tr>
  <td><code>/servidores</code></td>
  <td>GET</td>
  <td>TransparenciaServidoresConnector</td>
  <td><code>pagina</code> · <code>cpfServidor</code>, <code>nome</code>, <code>orgaoServidorExercicio</code> (opcionais)</td>
  <td>Servidores públicos federais: nome, órgão de lotação, órgão de exercício, cargo, função</td>
</tr>
</tbody>
</table>

### Como cada endpoint é usado

**`/ceis` e `/cnep`** — consultados em tempo real pelo `SanctionsAnalyzer` (pandas) e `SanctionsSparkAnalyzer` (Spark) durante o `investigate`. Não passam pela camada Bronze. O analyzer verifica cada CNPJ de fornecedor da CEAP e filtra por correspondência exata de CNPJ (`codigoFormatado` normalizado, sem pontuação) para evitar falsos positivos que a API pode retornar. Delay de **0.5s** entre consultas de CNPJs distintos.

**`/emendas`** — passa pelo pipeline completo (Bronze → Silver via `EmendasTransformer` ou `EmendasSparkTransformer`) e aparece no relatório como tabela separada. É opcional: o relatório exibe a seção apenas se o arquivo/tabela existir na Silver.

**`/contratos`** — usado diretamente pelo comando `argus check-contrato <codigo_orgao>`. A API pode retornar registros com IDs diferentes mas conteúdo igual; o CLI deduplica por número do contrato antes de exibir.

**`/servidores`** — usado diretamente pelo comando `argus check-servidor <cpf>`. O resultado é exibido como painel Rich com os campos: nome, CPF, órgão de lotação, órgão de exercício, cargo e função. Útil para cruzar os sócios dos fornecedores com a base de servidores federais.

### Timeout, validação e autenticação

Todos os endpoints do Portal da Transparência retornam listas diretas (`[{...}, {...}]`), sem wrapper `{"dados": [...]}`. A validação é `isinstance(data, list)`.

A autenticação é gerenciada pelo `TransparenciaBaseConnector`: injeta o header `chave-api-dados` automaticamente em todas as requisições filhas.

| Connector | Timeout | Validação |
|---|---|---|
| `TransparenciaCeisConnector` | 60s | resposta é lista |
| `TransparenciaCnepConnector` | 60s | resposta é lista |
| `TransparenciaEmendasConnector` | 60s | resposta é lista |
| `TransparenciaContratosConnector` | 60s | resposta é lista |
| `TransparenciaServidoresConnector` | 60s | resposta é lista |