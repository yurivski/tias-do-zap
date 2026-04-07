<div align="center">

# Arquitetura

Detalhes técnicos das Tias do Zap: estrutura do lakehouse, implementações pandas e Spark, Star Schema, herança de classes, fluxo do pipeline e fórmulas matemáticas dos analyzers.

</div>

---

## Sumário

- [Estrutura do projeto](#estrutura-do-projeto)
- [Implementações: pandas e Spark](#implementações-pandas-vs-spark)
- [Lakehouse Medallion](#lakehouse-medallion)
- [Herança de classes](#herança-de-classes)
- [Star Schema em detalhe](#star-schema-em-detalhe)
- [Fluxo do comando investigate](#fluxo-do-comando-investigate)
- [Fórmulas matemáticas dos alertas](#fórmulas-matemáticas-dos-alertas)

---

## Estrutura do projeto

```
tias-do-zap/
├── grupinho/
│   ├── creuza_conecta/                # Um arquivo por fonte de dados
│   │   ├── base.py                    # BaseConnector (ABC)
│   │   ├── camara_ceap.py             # Despesas CEAP
│   │   ├── camara_deputados.py        # Cadastro de deputados
│   │   ├── brasil_api_cnpj.py         # Dados de empresas
│   │   ├── transparencia_base.py      # Base autenticada para Portal da Transparência
│   │   ├── transparencia_contratos.py
│   │   ├── transparencia_servidores.py
│   │   ├── transparencia_emendas.py
│   │   ├── transparencia_ceis.py
│   │   └── transparencia_cnep.py
│   ├── shirley_transforma/
│   │   ├── base.py                    # BaseTransformer (ABC)
│   │   ├── silver/                    # Bronze → Silver (pandas → Parquet)
│   │   │   ├── ceap_transformer.py
│   │   │   ├── deputados_transformer.py
│   │   │   ├── cnpj_transformer.py
│   │   │   └── emendas_transformer.py
│   │   ├── silver_spark/              # Bronze → Silver (PySpark → Delta)
│   │   │   ├── ceap_transformer.py
│   │   │   ├── deputados_transformer.py
│   │   │   ├── cnpj_transformer.py
│   │   │   └── emendas_transformer.py
│   │   ├── gold/                      # Silver → Gold Star Schema (pandas → Parquet)
│   │   │   ├── dim_builders.py
│   │   │   └── fact_builders.py
│   │   └── gold_spark/                # Silver → Gold Star Schema (PySpark → Delta)
│   │       ├── dim_builders.py
│   │       └── fact_builders.py
│   ├── marli_analisa/                 # Regras de detecção (DuckDB/SQL)
│   │   ├── base.py                    # BaseAnalyzer (ABC)
│   │   ├── despesa_analise.py         # SPD-001, SPD-002
│   │   ├── fornecedor_analise.py      # SUP-001, SUP-002
│   │   └── sancoes_analise.py         # SUP-003, SUP-004
│   ├── marli_analisa_spark/           # Regras de detecção (PySpark/Delta)
│   │   ├── despesa_analise.py         # SPD-001, SPD-002
│   │   ├── fornecedor_analise.py      # SUP-001, SUP-002
│   │   └── sancoes_analise.py         # SUP-003, SUP-004
│   ├── neide_investiga/
│   │   └── consultas.py               # Resolução de nome → ID (DuckDB)
│   ├── marlene_pipeline/
│   │   └── enriquecimento.py          # Enriquecimento de fornecedores
│   ├── fatima_cli/
│   │   ├── cida.py                    # CLI com Typer (pandas/DuckDB)
│   │   └── rosa.py                    # CLI com Typer (PySpark/Delta Lake)
│   └── dulce_config/
│       ├── config.py                  # Pydantic Settings
│       └── logging.py                 # structlog setup
├── data/                              # Camadas do lakehouse (gitignored)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── lourdes_documenta/
├── .env                # Você cria e adiciona sua chaves de API (gitignored)
└── pyproject.toml
```

---

## Implementações: pandas e Spark

O projeto mantém duas implementações paralelas do pipeline de transformação e análise:

| Camada | pandas/DuckDB | PySpark/Delta |
|---|---|---|
| Silver transformers | `shirley_transforma/silver/` | `shirley_transforma/silver_spark/` |
| Gold builders | `shirley_transforma/gold/` | `shirley_transforma/gold_spark/` |
| Analyzers | `marli_analisa/` | `marli_analisa_spark/` |
| CLI | `fatima_cli/cida.py` | `fatima_cli/rosa.py` |
| Formato Silver/Gold | Parquet | Delta Lake |

A implementação **pandas/DuckDB** é a versão original: leve, sem dependências pesadas, ideal para exploração local. Usa pandas para transformação e DuckDB para consultas analíticas SQL direto nos Parquets.

A implementação de **PySpark/Delta** foi por motivo pessoal, queria treinar e consolidar algumas informações e resolvi implementar como uma versão separada. Use sempre a versão original com pandas, é mais rápido, apesar de ter muitos larápios para a gente sustentar em brasília, esses seres vivos possuem poucos dados disponíveis para executar o motor com Spark. Essa versão usa Apache Spark com Delta Lake como formato de armazenamento. O Delta substitui o Parquet adicionando suporte a transações ACID, versionamento e operações de upsert.

O `fatima_cli/rosa.py` organiza o pipeline completo em sub-comandos Typer:

```
Tias_do_Zap ingest ceap <deputado_id>        ← ingere despesas CEAP
Tias_do_Zap ingest deputados                 ← ingere cadastro de parlamentares
Tias_do_Zap ingest fornecedores              ← enriquece CNPJs via BrasilAPI
Tias_do_Zap ingest emendas <nome_autor>      ← ingere emendas do parlamentar
Tias_do_Zap transform silver                 ← Bronze → Silver (Delta)
Tias_do_Zap transform gold                   ← Silver → Gold Star Schema (Delta)
Tias_do_Zap check-servidor <cpf>             ← verifica se CPF é servidor federal
Tias_do_Zap check-contrato <codigo_orgao>    ← lista contratos de um órgão
Tias_do_Zap investigate <nome_ou_id>         ← pipeline completo + relatório de risco
```

O comando `investigate` é o pipeline completo: executa ingestão, transformação, enriquecimento, constrói o Star Schema e roda todos os analyzers em sequência, gerando um relatório de risco no terminal via Rich.

A `SparkSession` é inicializada uma única vez e reutilizada em todo o pipeline via `getOrCreate()`. Os JARs do Delta Lake são lidos de `~/.ivy2.5.2/jars/` para evitar que o Ivy faça download em cada execução.

---

## Lakehouse Medallion

### Bronze

Armazena o JSON exatamente como veio da API. Cada ingestão cria um novo arquivo com timestamp — nada é sobrescrito.

```
data/bronze/
├── camara_ceap/
│   └── 2026-03-26/
│       └── 201552.json          ← Exemplo: 449 despesas do deputado 131313
├── camara_deputados/
│   └── 2026-03-26/
│       └── 232523.json          ← Exemplo: 722 deputados da legislatura 57
├── brasil_api_cnpj/
│   └── 2026-03-26/
│       ├── 013027.json          ← Exemplo: dados de 1 empresa
│       ├── 013028.json
│       └── ...                  ← 1 arquivo por CNPJ consultado
└── transparencia_emendas/
    └── 2026-03-29/
        └── 201552.json          ← Exemplo: 7 emendas do autor
```

### Silver

Transforma os JSONs em tabelas com schema definido. Cada transformer aplica:

1. **Seleção de colunas** — descarta campos desnecessários
2. **Rename para snake_case** — `cnpjCpfFornecedor` vira `cnpj_cpf_fornecedor`
3. **Conversão de tipos** — strings para datetime, floats, ints
4. **Limpeza** — strings vazias para null, duplicatas removidas
5. **Ordenação** — por chave natural para facilitar inspeção

Na implementação **pandas**, a Silver é salva como Parquet (arquivo único por tabela):

```
data/silver/
├── ceap_despesas.parquet        ← despesas CEAP limpas
├── deputados.parquet            ← cadastro de deputados
├── cnpj_empresas.parquet        ← dados de empresas (BrasilAPI)
├── cnpj_socios.parquet          ← sócios extraídos do QSA
└── emendas.parquet              ← emendas parlamentares
```

Na implementação **Spark**, a Silver é salva como Delta Lake (diretório por tabela, com `_delta_log/` interno), no mesmo diretório `data/silver/`:

```
data/silver/
├── ceap_despesas/
│   └── _delta_log/              ← transaction log do Delta
├── deputados/
│   └── _delta_log/
├── cnpj_empresas/
│   └── _delta_log/
├── cnpj_socios/
│   └── _delta_log/
└── emendas/
    └── _delta_log/
```

O presence check do `_delta_log/` é usado pelo CLI para decidir se uma tabela opcional (como sócios ou emendas) foi ingerida antes de tentar lê-la:

```python
socios_path = silver / "cnpj_socios"
if (socios_path / "_delta_log").exists():
    df_socios = spark.read.format("delta").load(str(socios_path))
```

Caso especial: os valores monetários da API de emendas vêm no formato brasileiro (`"500.000,00"` como string). O transformer remove pontos de milhar, troca vírgula por ponto, e converte para float:

```python
# "2.160.655,00" → "2160655.00" → 2160655.0
df[col] = df[col].str.replace(".", "", regex=False)
df[col] = df[col].str.replace(",", ".", regex=False)
df[col] = pd.to_numeric(df[col], errors="coerce")
```

### Gold

Modela os dados em Star Schema. Detalhado na seção abaixo.

Na implementação **Spark**, a Gold também é Delta Lake, no mesmo diretório `data/gold/`. Cada tabela é um diretório:

```
data/gold/
├── dim_parlamentar/             ← deputado_id, nome, partido, UF, legislatura
│   └── _delta_log/
├── dim_fornecedor/              ← cnpj_cpf, nome do fornecedor
│   └── _delta_log/
├── dim_categoria_despesa/       ← tipo de despesa (descrição)
│   └── _delta_log/
└── fact_despesas_ceap/          ← despesas com FKs para as três dimensões
    └── _delta_log/
```

As surrogate keys na implementação Spark são geradas com `monotonically_increasing_id()` em vez de `range()`:

```python
dim = dim.withColumn("parlamentar_key", F.monotonically_increasing_id() + 1)
```

A `cnpj_empresas` não tem tabela Gold — os analyzers Spark a leem diretamente da Silver, pois ela não faz parte do Star Schema de despesas CEAP.

---

## Herança de classes

O projeto usa Abstract Base Classes (ABC) para definir contratos que cada componente deve seguir.

### Connectors

```
BaseConnector (ABC)
├── CamaraCeapConnector
├── CamaraDeputadosConnector
├── BrasilApiCNPJConnector
└── TransparenciaBaseConnector
    ├── TransparenciaContratosConnector
    ├── TransparenciaServidoresConnector
    ├── TransparenciaEmendasConnector
    ├── TransparenciaCeisConnector
    └── TransparenciaCnepConnector
```

O `BaseConnector` define 5 métodos abstratos que toda classe filha concreta implementa: `name`, `source_url`, `fetch`, `validate` e `save_bronze`.

O `TransparenciaBaseConnector` é uma classe intermediária, herda de `BaseConnector` e adiciona autenticação via header (`_get_headers`) e requisição autenticada (`_make_request`). Os 5 connectors do Portal da Transparência herdam dela em vez de diretamente do `BaseConnector`, evitando repetir a lógica de autenticação.

### Transformers

Ambas as implementações herdam do mesmo `BaseTransformer`:

```
BaseTransformer (ABC)
├── CeapTransformer              ← pandas → Parquet      (silver/)
├── DeputadosTransformer         ← pandas → Parquet      (silver/)
├── CNPJTransformer              ← pandas → Parquet      (silver/)
├── EmendasTransformer           ← pandas → Parquet      (silver/)
├── CeapSparkTransformer         ← PySpark → Delta       (silver/)
├── DeputadosSparkTransformer    ← PySpark → Delta       (silver/)
├── CNPJSparkTransformer         ← PySpark → Delta       (silver/)
└── EmendasSparkTransformer      ← PySpark → Delta       (silver/)
```

Os builders Gold não herdam de `BaseTransformer` — são funções puras (`build_dim_parlamentar`, `build_dim_fornecedor`, `build_dim_categoria_despesa`, `build_fact_despesas_ceap`) em módulos separados para cada implementação (`gold/` e `gold_spark/`).

### Analyzers

Ambas as implementações herdam do mesmo `BaseAnalyzer`:

```
BaseAnalyzer (ABC)
├── SpendingAnalyzer          ← SQL via DuckDB nos Parquets (marli_analisa/)
├── SupplierAnalyzer          ← SQL via DuckDB nos Parquets (marli_analisa/)
├── SanctionsAnalyzer         ← API calls em tempo real    (marli_analisa/)
├── SpendingSparkAnalyzer     ← PySpark/Delta              (marli_analisa_spark/)
├── SupplierSparkAnalyzer     ← PySpark/Delta              (marli_analisa_spark/)
└── SanctionsSparkAnalyzer    ← PySpark/Delta + API calls  (marli_analisa_spark/)
```

O `SanctionsSparkAnalyzer` é o único analyzer assíncrono: lê os fornecedores via PySpark, depois consulta o CEIS e CNEP por CNPJ de forma assíncrona com `asyncio`. O método síncrono `analyze()` envelopa o código async com `asyncio.run()` para compatibilidade com o contrato do `BaseAnalyzer`.

---

## Star Schema em detalhe

### O modelo

O Star Schema organiza dados em dois tipos de tabela:

**Tabelas dimensão** guardam contexto descritivo. Mudam pouco. Cada linha tem uma Primary Key (PK) que identifica o registro unicamente.

**Tabelas fato** guardam eventos com métricas, coisas que aconteceram. Cada linha tem Foreign Keys (FK) que apontam para as dimensões, conectando o evento ao seu contexto.

### Tabelas das Tias do Zap

```
                    dim_parlamentar
                    PK: parlamentar_key
                    nome, partido, uf
                         │
                         │ FK: parlamentar_key
                         │
fact_despesas_ceap ──────┼────── dim_fornecedor
PK: despesa_key          │       PK: fornecedor_key
valor_liquido            │       nome, cnpj
valor_glosa              │
data_documento           │
                         │ FK: categoria_key
                         │
                    dim_categoria_despesa
                    PK: categoria_key
                    descricao
```

### Primary Key e Foreign Key

A PK identifica unicamente cada linha. Na `dim_parlamentar`, `parlamentar_key = 13` é o Larápio Silva. Não existe outro registro com a mesma key.

A FK é uma coluna na tabela fato que aponta para a PK de uma dimensão. Na `fact_despesas_ceap`, `parlamentar_key = 13` diz "essa despesa pertence ao deputado 13". O JOIN conecta as duas:

```sql
SELECT dp.nome_parlamentar, SUM(f.valor_liquido) as total
FROM fact_despesas_ceap f
JOIN dim_parlamentar dp
    ON f.parlamentar_key = dp.parlamentar_key
GROUP BY dp.nome_parlamentar
```

O SQL pega cada linha da fato, segue o `parlamentar_key` até a dimensão, traz o nome, e soma os valores.

### Surrogate Key e Natural Key

Natural key é um identificador que já existe nos dados: CNPJ, CPF, `deputado_id` da API. Surrogate key é um inteiro sequencial gerado pelo sistema (`parlamentar_key = 1, 2, 3...`).

A Dona Shirley usa surrogate keys por três motivos:

1. **Performance** — JOINs por inteiro são mais rápidos que por string CNPJ
2. **Estabilidade** — se a API mudar o formato do ID, a surrogate key não muda
3. **Independência** — o modelo não depende de nenhuma fonte externa para identificar registros

A geração das surrogate keys no código:

```python
# pandas: inteiros sequenciais começando em 1
dim.insert(0, 'parlamentar_key', range(1, len(dim) + 1))

# PySpark: IDs monotonicamente crescentes começando em 1
dim = dim.withColumn("parlamentar_key", F.monotonically_increasing_id() + 1)
```

---

## Fluxo do comando investigate

O `Tias_do_Zap investigate` é o pipeline de ponta a ponta. O fluxo completo, na ordem de execução:

```
1. ingest deputados          ← lista todos os deputados da legislatura
2. transform silver          ← Bronze → Silver (Delta) com deputados
3. resolve query             ← nome parcial ou ID numérico → deputado_id
4. ingest ceap               ← despesas do deputado resolvido
5. transform silver          ← Bronze → Silver com CEAP
6. ingest fornecedores       ← enriquece CNPJs via BrasilAPI (delay 1.5s)
7. transform silver          ← Bronze → Silver com dados de empresas/sócios
8. transform gold            ← Silver → Gold Star Schema (Delta)
9. SpendingSparkAnalyzer     ← SPD-001, SPD-002
10. SupplierSparkAnalyzer    ← SUP-001, SUP-002
11. SanctionsSparkAnalyzer   ← SUP-003, SUP-004 (API em tempo real)
12. relatório                ← resumo financeiro + top fornecedores + alertas
```

A resolução do deputado na etapa 3 faz uma busca por `LIKE '%query%'` na tabela Delta `deputados` da Silver. Se encontrar mais de um resultado, lista os matches e pede confirmação interativa via `typer.prompt`. Se for exatamente um match, segue automaticamente.

O relatório exibe, na ordem: painel do investigado → resumo financeiro → top 10 fornecedores → sócios dos fornecedores (se disponível) → emendas parlamentares (se disponível) → alertas com score e classificação de risco.

O `--skip-ingest` pula as etapas 1–8 e reutiliza os dados já presentes nas camadas Silver e Gold, útil para re-executar apenas a análise sem refazer todo o pipeline.

---

## Fórmulas matemáticas dos alertas

### SPD-001: Concentração de fornecedor

Detecta quando um único fornecedor concentra mais de 40% do total de gastos de um deputado.

```
percentual = (valor_fornecedor / valor_total_deputado) * 100
```

Se `percentual > 40%`, dispara o alerta.

Essa conta é feita via SQL com CTE. A CTE `gastos_por_fornecedor` agrupa por deputado e fornecedor, a CTE `total_deputado` soma o total por deputado, e a query final divide um pelo outro:

```sql
WITH gastos_por_fornecedor AS (
    SELECT parlamentar_key, fornecedor_key,
           SUM(valor_liquido) AS total_fornecedor
    FROM fact_despesas_ceap
    GROUP BY parlamentar_key, fornecedor_key
),
total_deputado AS (
    SELECT parlamentar_key,
           SUM(total_fornecedor) AS total_geral
    FROM gastos_por_fornecedor
    GROUP BY parlamentar_key
)
SELECT ...
WHERE gf.total_fornecedor / td.total_geral > 0.40
```

*Esse threshold de 40% é uma regra de negócio definida no projeto. Em auditoria, concentração acima de 30% já levanta suspeita. Fonte: Google.*

### SPD-002: Nota fiscal acima de 3 desvios padrão

Detecta notas fiscais individuais cujo valor é estatisticamente atípico em relação ao padrão de gastos do deputado.

```
limite = média + (3 × desvio_padrão)
```

Se `valor_nota > limite`, dispara o alerta.

A média (μ) é a soma de todos os valores dividida pela quantidade. O desvio padrão (σ) mede o quanto os valores se afastam da média. Na distribuição normal, 99.7% dos dados ficam dentro de 3 desvios, um valor fora desse range é estatisticamente raro.

Em ambas as implementações (**pandas/DuckDB** e **Spark**), a estatística é calculada globalmente sobre todos os registros da tabela antes do filtro por deputado. O limite é global, garantindo uma baseline mais robusta:

Na implementação **Spark**:

```python
stats = fact.agg(
    F.avg("valor_liquido").alias("media"),
    F.stddev("valor_liquido").alias("desvio"),
).collect()[0]

limite = stats.media + (3 * stats.desvio)

# Só depois filtra pelo deputado e compara com o limite global
fact = fact.join(keys, on="parlamentar_key", how="inner")
resultado = fact.filter(F.col("valor_liquido") > limite)
```

Exemplo real: média = R$920,42, desvio = R$1.653,20, limite = R$5.880,02. As 9 notas de R$7.000 do *(Nome censurado por motivos óbvios)* estão acima do limite.

### SUP-001: Fornecedor recém-criado

Compara a data de abertura da empresa (BrasilAPI) com a data da primeira despesa CEAP que ela recebeu. Se a empresa foi criada menos de 12 meses antes da primeira despesa, dispara o alerta.

Na implementação **pandas/DuckDB**:

```sql
DATEDIFF('month',
    CAST(e.data_abertura AS DATE),
    CAST(pd.primeira_data AS DATE)
) < 12
```

Na implementação **Spark**, usa `months_between` + `floor` para replicar o mesmo comportamento:

```python
.withColumn(
    "meses_diferenca",
    F.floor(F.months_between(
        F.col("primeira_data").cast("date"),
        F.col("data_abertura").cast("date"),
    )).cast("int"),
)
.filter(F.col("meses_diferenca") < 12)
```

Em ambos os casos, `primeira_despesa` é a data mais antiga (`MIN(data_documento)`) de cada fornecedor. O JOIN com as empresas da Silver traz a `data_abertura`. CPFs são excluídos do check, apenas CNPJs (14+ caracteres) são verificados.

### SUP-002: Capital social incompatível

Compara o capital social declarado na Receita Federal (via BrasilAPI) com o total que a empresa recebeu em despesas CEAP. Se recebeu mais do que o capital social, dispara o alerta.

```sql
WHERE e.capital_social > 0
    AND gf.total_recebido > e.capital_social
```

O percentual é calculado com `NULLIF` para evitar divisão por zero:

```sql
ROUND(gf.total_recebido / NULLIF(e.capital_social, 0) * 100, 2)
    AS percentual_do_capital
```

Exemplo real: *(NOME DA EMPRESA CENSURADO POR MOTIVOS ÓBVIOS)* tem capital social de R$10.000 e recebeu R$12.600 — 126% do capital.

### SUP-003 e SUP-004: Fornecedor em listas de sanções

Consulta em tempo real o CEIS e CNEP do Portal da Transparência por CNPJ. Se a API retornar registros, filtra por correspondência exata de CNPJ (a API pode retornar resultados parciais):

```python
cnpj_sancionado = sancao.get("sancionado", {}).get("codigoFormatado", "")
cnpj_sancionado = cnpj_sancionado.replace(".", "").replace("/", "").replace("-", "")
if cnpj_sancionado == cnpj:
    # alerta confirmado
```

O `break` após o primeiro match garante no máximo um alerta por fornecedor por lista, evita que uma empresa com múltiplas sanções gere alertas duplicados.

### Classificação de risco

A soma dos scores individuais define o nível de risco:

| Score | Classificação | Interpretação |
|---|---|---|
| 0–3 | BAIXO | Poucos indicadores, dentro do esperado |
| 4–7 | MÉDIO | Múltiplos indicadores, merece atenção |
| 8–12 | ALTO | Concentração significativa de anomalias |
| 13+ | CRÍTICO | Padrão fortemente anômalo |

Scores por tipo de alerta:

| Alerta | Score |
|---|---|
| SPD-001 | 2 |
| SPD-002 | 2 |
| SUP-001 | 2 |
| SUP-002 | 2 |
| SUP-003 | 3 |
| SUP-004 | 3 |

Os alertas de sanções (SUP-003/004) têm score maior porque indicam uma irregularidade já formalizada pelo governo.