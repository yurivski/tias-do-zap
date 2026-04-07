<div align="center">

# Comandos

Lista completa de comandos das Tias do Zap, ordem de execução e fluxo interno.

</div>

---

## Sumário

- [Engines: pandas e Spark](#engines-pandas-vs-spark)
- [Comando principal: investigate](#comando-principal-investigate)
- [Comandos de ingestão](#comandos-de-ingestão)
- [Comandos de transformação](#comandos-de-transformação)
- [Comandos de consulta](#comandos-de-consulta)
- [Ordem de execução](#ordem-de-execução)
- [Fluxo interno do investigate](#fluxo-interno-do-investigate)

---

## Engines: pandas e Spark

As Tias do Zap têm dois CLIs com os mesmos comandos, mas engines diferentes:

| | pandas/DuckDB | PySpark/Delta |
|---|---|---|
| Módulo | `grupinho.fatima_cli.cida` | `grupinho.fatima_cli.rosa` |
| Invocação | `Tias_do_Zap <comando>` | `python -m grupinho.fatima_cli.rosa <comando>` |
| Silver/Gold | Parquet (`.parquet`) | Delta Lake (diretório + `_delta_log/`) |
| Analyzers | DuckDB SQL sobre Parquet | PySpark sobre tabelas Delta |
| Resolução de nome | `neide_investiga/consultas.py` via DuckDB | LIKE direto na Delta de deputados |
| SparkSession | não usa | inicializada uma vez, reutilizada |

O entry point padrão (`Tias_do_Zap`) aponta para o engine pandas/DuckDB, conforme `pyproject.toml`:

```toml
[project.scripts]
Tias_do_Zap = "grupinho.fatima_cli.cida:app"
```

Os comandos e parâmetros são idênticos nos dois CLIs. A diferença está nos artefatos gerados e nas dependências de runtime, o engine Spark exige PySpark e Delta Lake instalados, além dos JARs em `~/.ivy2.5.2/jars/`.

---

## Comando principal: investigate

Executa o pipeline completo e gera o relatório de investigação.

```bash
# Por nome (busca parcial, case insensitive)
Tias_do_Zap investigate "Larapio" --ano 2024

# Por ID da API da Câmara
Tias_do_Zap investigate 131313 --ano 2024

# Pulando a ingestão (usa dados já armazenados)
Tias_do_Zap investigate "Larapio" --ano 2024 --skip-ingest
```

| Parâmetro | Tipo | Obrigatório | Padrão | Descrição |
|---|---|---|---|---|
| `query` | str | sim | — | Nome ou ID do deputado |
| `--ano` | int | não | None | Ano das despesas CEAP |
| `--legislatura` | int | não | 57 | Legislatura (57 = 2023-2027) |
| `--skip-ingest` | bool | não | False | Pula a ingestão e usa dados locais |

Quando `query` é texto, o sistema busca na Silver de deputados por correspondência parcial. Se encontrar um único resultado, usa automaticamente. Se encontrar vários, exibe uma lista numerada para seleção.

---

## Comandos de ingestão

Cada comando consulta uma API específica e salva o JSON bruto na camada Bronze.

### ingest deputados

```bash
Tias_do_Zap ingest deputados --legislatura 57
```

Consulta a API da Câmara dos Deputados e ingere todos os parlamentares da legislatura com paginação automática.

- **API**: Câmara dos Deputados
- **Endpoint**: `GET /api/v2/deputados`
- **Paginação**: automática (100 registros por página, busca todas)
- **Saída**: `data/bronze/camara_deputados/{data}/{hora}.json`

### ingest ceap

```bash
Tias_do_Zap ingest ceap 131313 --ano 2024
```

Ingere todas as despesas da Cota Parlamentar de um deputado específico.

- **API**: Câmara dos Deputados
- **Endpoint**: `GET /api/v2/deputados/{id}/despesas`
- **Paginação**: automática (100 por página, busca todas)
- **Saída**: `data/bronze/camara_ceap/{data}/{hora}.json`

| Parâmetro | Tipo | Obrigatório | Padrão | Descrição |
|---|---|---|---|---|
| `deputado_id` | int | sim | — | ID do deputado na API da Câmara |
| `--ano` | int | não | None | Filtrar despesas por ano |

### ingest fornecedores

```bash
Tias_do_Zap ingest fornecedores --delay 1.5
```

Lê os CNPJs únicos da Silver de despesas CEAP e consulta cada um na BrasilAPI para obter dados da empresa (sócios, capital social, data de abertura).

- **API**: BrasilAPI
- **Endpoint**: `GET /api/cnpj/v1/{cnpj}`
- **Rate limit**: 1 requisição por `delay` segundos (padrão: 1.0s)
- **Filtragem**: ignora CPFs (< 14 dígitos), consulta apenas CNPJs
- **Saída**: `data/bronze/brasil_api_cnpj/{data}/{hora}.json` (um arquivo por empresa)

| Parâmetro | Tipo | Obrigatório | Padrão | Descrição |
|---|---|---|---|---|
| `--delay` | float | não | 1.0 | Segundos entre requisições |

### ingest emendas

```bash
Tias_do_Zap ingest emendas "LARAPIO SILVA" --ano 2024
```

Ingere as emendas parlamentares de um autor específico.

- **API**: Portal da Transparência
- **Endpoint**: `GET /api-de-dados/emendas`
- **Autenticação**: header `chave-api-dados` com token do `.env`
- **Saída**: `data/bronze/transparencia_emendas/{data}/{hora}.json`

| Parâmetro | Tipo | Obrigatório | Padrão | Descrição |
|---|---|---|---|---|
| `nome_autor` | str | sim | — | Nome do parlamentar |
| `--ano` | int | não | None | Filtrar por ano |

---

## Comandos de transformação

### transform silver

```bash
Tias_do_Zap transform silver
```

Lê todos os JSONs da Bronze e gera tabelas Silver. Executa 4 transformers em sequência. Cada transformer limpa os dados: renomeia colunas para snake_case, converte tipos (timestamps, floats), substitui strings vazias por null, remove duplicatas. Se a Bronze não tiver dados para um transformer (ex: emendas não foram ingeridas ainda), o transformer é pulado com aviso, não interrompe os demais.

**Engine pandas/DuckDB** — transformers pandas, saída em Parquet:

1. `CeapTransformer` → `data/silver/ceap_despesas.parquet`
2. `DeputadosTransformer` → `data/silver/deputados.parquet`
3. `CNPJTransformer` → `data/silver/cnpj_empresas.parquet` + `data/silver/cnpj_socios.parquet`
4. `EmendasTransformer` → `data/silver/emendas.parquet`

**Engine PySpark/Delta** — transformers Spark, saída em Delta Lake:

1. `CeapSparkTransformer` → `data/silver/ceap_despesas/`
2. `DeputadosSparkTransformer` → `data/silver/deputados/`
3. `CNPJSparkTransformer` → `data/silver/cnpj_empresas/` + `data/silver/cnpj_socios/`
4. `EmendasSparkTransformer` → `data/silver/emendas/`

### transform gold

```bash
Tias_do_Zap transform gold
```

Constrói o Star Schema a partir da Silver. Dimensões são criadas antes da fato.

**Engine pandas/DuckDB** — builders pandas, saída em Parquet:

1. `dim_parlamentar` → `data/gold/dim_parlamentar.parquet`
2. `dim_fornecedor` → `data/gold/dim_fornecedor.parquet`
3. `dim_categoria_despesa` → `data/gold/dim_categoria_despesa.parquet`
4. `fact_despesas_ceap` → `data/gold/fact_despesas_ceap.parquet`

**Engine PySpark/Delta** — builders Spark, saída em Delta Lake:

1. `dim_parlamentar` → `data/gold/dim_parlamentar/`
2. `dim_fornecedor` → `data/gold/dim_fornecedor/`
3. `dim_categoria_despesa` → `data/gold/dim_categoria_despesa/`
4. `fact_despesas_ceap` → `data/gold/fact_despesas_ceap/`

---

## Comandos de consulta

### check-servidor

```bash
Tias_do_Zap check-servidor 13131313131
```

Verifica se um CPF pertence a um servidor público federal ativo, consultando o Portal da Transparência em tempo real.

- **API**: Portal da Transparência
- **Endpoint**: `GET /api-de-dados/servidores`
- **Autenticação**: header `chave-api-dados` com token do `.env`
- **Saída**: painel Rich com nome, órgão de lotação, órgão de exercício, cargo e função

| Parâmetro | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `cpf` | str | sim | CPF completo do servidor (11 dígitos) |

Útil após ver os sócios dos fornecedores no relatório do `investigate` — o próprio relatório sugere o comando.

### check-contrato

```bash
Tias_do_Zap check-contrato 26000 --pagina 1
```

Lista contratos federais de um órgão governamental específico.

- **API**: Portal da Transparência
- **Endpoint**: `GET /api-de-dados/contratos`
- **Autenticação**: header `chave-api-dados` com token do `.env`
- **Saída**: tabela Rich com número, fornecedor, objeto e valor do contrato
- **Deduplicação**: contratos com mesmo número são exibidos apenas uma vez (a API pode retornar duplicatas)

| Parâmetro | Tipo | Obrigatório | Padrão | Descrição |
|---|---|---|---|---|
| `codigo_orgao` | str | sim | — | Código do órgão (SIAPE) |
| `--pagina` | int | não | 1 | Página de resultados |

---

## Ordem de execução

A ordem importa porque cada etapa depende da anterior. Os comandos de ingestão são iguais nos dois engines, a diferença está no que `transform silver` e `transform gold` produzem.

### Execução manual (passo a passo)

**Engine pandas/DuckDB** (`Tias_do_Zap`):

```
1. Tias_do_Zap ingest deputados          → Bronze (deputados)
2. Tias_do_Zap transform silver          → Silver: deputados.parquet
3. Tias_do_Zap ingest ceap {id}          → Bronze (despesas)
4. Tias_do_Zap transform silver          → Silver: ceap_despesas.parquet
5. Tias_do_Zap ingest fornecedores       → Bronze (CNPJs)
6. Tias_do_Zap transform silver          → Silver: cnpj_empresas.parquet, cnpj_socios.parquet
7. Tias_do_Zap ingest emendas "{nome}"   → Bronze (emendas)    ← opcional
8. Tias_do_Zap transform silver          → Silver: emendas.parquet   ← opcional
9. Tias_do_Zap transform gold            → Gold: *.parquet (Star Schema)
```

**Engine PySpark/Delta** (`python -m grupinho.fatima_cli.rosa`):

```
1. ... ingest deputados            → Bronze (deputados)
2. ... transform silver            → Silver Delta: deputados/
3. ... ingest ceap {id}            → Bronze (despesas)
4. ... transform silver            → Silver Delta: ceap_despesas/
5. ... ingest fornecedores         → Bronze (CNPJs)
6. ... transform silver            → Silver Delta: cnpj_empresas/, cnpj_socios/
7. ... ingest emendas "{nome}"     → Bronze (emendas)    ← opcional
8. ... transform silver            → Silver Delta: emendas/    ← opcional
9. ... transform gold              → Gold Delta: */ (Star Schema)
```

Os passos 7 e 8 são opcionais em ambos os engines. Se as emendas não forem ingeridas, o relatório simplesmente não exibe a seção de emendas.

### Execução automática (investigate)

```bash
# pandas/DuckDB (entry point padrão)
Tias_do_Zap investigate "Brunini" --ano 2024

# PySpark/Delta
python -m grupinho.fatima_cli.rosa investigate "Brunini" --ano 2024
```

Executa os passos 1–6 e 9 automaticamente (sem emendas), mais os analyzers e o relatório. Para incluir emendas, execute `argus ingest emendas` e `argus transform silver` manualmente antes ou depois.

---

## Fluxo interno do investigate

Os dois engines executam os mesmos 6 passos visíveis no terminal, mas diferem na implementação interna. As etapas de ingestão são idênticas. As diferenças estão na transformação, resolução de nome, analyzers e relatório.

### Engine pandas/DuckDB (`Tias_do_Zap investigate "Larapio"`)

```
[1/6] Ingerindo dados de parlamentares
      └─ API Câmara: GET /api/v2/deputados (8 páginas, 722 registros)
      └─ Salva JSONs em data/bronze/camara_deputados/

[2/6] Primeira transformação de dados
      └─ CeapTransformer, DeputadosTransformer, CNPJTransformer, EmendasTransformer
      └─ Gera Silver Parquet: deputados.parquet (necessário para resolver o nome)

      ── Resolve "larapio" → search_deputado_by_name() via DuckDB sobre deputados.parquet
         └─ 1 resultado → prossegue automaticamente com ID 131313
         └─ N resultados → exibe lista numerada e aguarda seleção do usuário

[3/6] Ingerindo dados do CEAP
      └─ API Câmara: GET /api/v2/deputados/131313/despesas (5 páginas, 449 registros)
      └─ Salva JSON em data/bronze/camara_ceap/

[4/6] Transformando despesas e enriquecendo fornecedores
      └─ transform silver → Silver Parquet: ceap_despesas.parquet
      └─ ingest fornecedores → BrasilAPI: GET /api/cnpj/v1/{cnpj} (delay 1.5s)
      └─ Salva JSONs em data/bronze/brasil_api_cnpj/

[5/6] Processando dados de fornecedores
      └─ transform silver → Silver Parquet: cnpj_empresas.parquet, cnpj_socios.parquet

[6/6] Construindo star schema gold
      └─ transform gold → Gold Parquet:
         dim_parlamentar.parquet, dim_fornecedor.parquet,
         dim_categoria_despesa.parquet, fact_despesas_ceap.parquet

[INFO] 1/2 ≫ Executando a detecção de anomalias
      └─ SpendingAnalyzer: SPD-001, SPD-002  (DuckDB SQL sobre Parquet)
      └─ SupplierAnalyzer: SUP-001, SUP-002  (DuckDB SQL + cnpj_empresas.parquet)
      └─ SanctionsAnalyzer: SUP-003, SUP-004 (DuckDB + API em tempo real)
         └─ API Transparência: GET /api-de-dados/ceis (por CNPJ, delay 0.5s)
         └─ API Transparência: GET /api-de-dados/cnep (por CNPJ, delay 0.5s)

[INFO] 2/2 ≫ Gerando relatório
      └─ DuckDB lê os Parquets da Gold para montar o relatório
      └─ Painel: investigado (nome, partido, UF)
      └─ Painel: resumo financeiro (total de despesas, valor total, média, fornecedores únicos)
      └─ Tabela: top 10 fornecedores por valor total
      └─ Tabela: sócios dos fornecedores (se cnpj_socios.parquet existir na Silver)
      └─ Tabela: emendas parlamentares (se emendas.parquet existir na Silver)
      └─ Painel: nível de risco (score total + classificação BAIXO/MÉDIO/ALTO/CRÍTICO)
      └─ Tabela: alertas detectados (código, descrição, score, detalhe)
```

### Engine PySpark/Delta (`python -m argus.cli.argus investigate "Larapio"`)

```
[1/6] Ingerindo dados de parlamentares
      └─ (idêntico ao pandas)

[2/6] Primeira transformação de dados
      └─ CeapSparkTransformer, DeputadosSparkTransformer, CNPJSparkTransformer,
         EmendasSparkTransformer
      └─ Gera Silver Delta: deputados/ (necessário para resolver o nome)

      ── Resolve "Larapio" → LIKE '%Larapio%' via PySpark na Delta de deputados
         └─ 1 resultado → prossegue automaticamente com ID 131313
         └─ N resultados → exibe lista numerada e aguarda seleção do usuário

[3/6] Ingerindo dados do CEAP
      └─ (idêntico ao pandas)

[4/6] Transformando despesas e enriquecendo fornecedores
      └─ transform silver → Silver Delta: ceap_despesas/
      └─ ingest fornecedores → BrasilAPI: GET /api/cnpj/v1/{cnpj} (delay 1.5s)
      └─ Salva JSONs em data/bronze/brasil_api_cnpj/

[5/6] Processando dados de fornecedores
      └─ transform silver → Silver Delta: cnpj_empresas/, cnpj_socios/

[6/6] Construindo star schema gold
      └─ transform gold → Gold Delta:
         dim_parlamentar/, dim_fornecedor/,
         dim_categoria_despesa/, fact_despesas_ceap/

[INFO] 1/2 ≫ Executando a detecção de anomalias
      └─ SpendingSparkAnalyzer: SPD-001, SPD-002  (PySpark sobre tabelas Delta)
      └─ SupplierSparkAnalyzer: SUP-001, SUP-002  (PySpark + leitura da Silver Delta)
      └─ SanctionsSparkAnalyzer: SUP-003, SUP-004 (PySpark + API em tempo real, asyncio)
         └─ API Transparência: GET /api-de-dados/ceis (por CNPJ, delay 0.5s)
         └─ API Transparência: GET /api-de-dados/cnep (por CNPJ, delay 0.5s)

[INFO] 2/2 ≫ Gerando relatório
      └─ PySpark lê as tabelas Delta da Gold para montar o relatório
      └─ Painel: investigado (nome, partido, UF)
      └─ Painel: resumo financeiro (total de despesas, valor total, média, fornecedores únicos)
      └─ Tabela: top 10 fornecedores por valor total
      └─ Tabela: sócios dos fornecedores (se cnpj_socios/_delta_log/ existir na Silver)
      └─ Tabela: emendas parlamentares (se emendas/_delta_log/ existir na Silver)
      └─ Painel: nível de risco (score total + classificação BAIXO/MÉDIO/ALTO/CRÍTICO)
      └─ Tabela: alertas detectados (código, descrição, score, detalhe)
```

Com `--skip-ingest`, as etapas `[1/6]` a `[6/6]` são puladas nos dois engines. O sistema vai direto para os analyzers usando os dados já presentes em `data/silver/` e `data/gold/`.