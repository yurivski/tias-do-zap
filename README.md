<div align="center">

<h1><img src="img/tias_do_zap.png" alt="Tias do Zap" width="170" align="absmiddle">&nbsp;&nbsp;</h1>

#### Grupinho Open Source de inteligência e auditoria para dados abertos de parlamentares brasileiros.  

<br>

As Tias do Zap ingerem dados de APIs públicas brasileiras, transformam em um lakehouse organizado, e detectam anomalias em gastos parlamentares usando regras determinísticas. Cada alerta é verificável com dados públicos.
</div>

---

## Sumário

- [O que as Tias fazem](#o-que-as-tias-fazem)
- [Como funciona](#como-funciona)
- [Stack técnica](#stack-técnica)
- [Instalação](#instalação)
- [Logs](#logs)
- [Uso rápido](#uso-rápido)
- [Arquitetura Medallion](#arquitetura-medallion)
- [Star Schema](#star-schema)
- [Sistema de alertas](#sistema-de-alertas)
- [Documentação detalhada](#documentação-detalhada)

---

## O que as Tias fazem

Dado o nome de um parlamentar, as Tias do Zap:

1. Buscam o cadastro do parlamentar na API da Câmara dos Deputados
2. Ingerem todas as despesas da Cota Parlamentar (CEAP) com paginação automática
3. Consultam os CNPJs dos fornecedores na BrasilAPI para obter sócios, capital social e data de abertura
4. Verificam se algum fornecedor consta no CEIS ou CNEP (listas de empresas sancionadas pelo governo)
5. Ingerem as emendas parlamentares do Portal da Transparência
6. Transformam tudo em um lakehouse com três camadas (Bronze, Silver, Gold)
7. Modelam os dados em Star Schema para consultas analíticas
8. Rodam regras de detecção de anomalias e geram um relatório com classificação de risco  

*Obs.: tudo em CLI*

---

## Como funciona

Um comando:

```bash
Tias_do_Zap investigate "Larapio" --ano 2024
```

O sistema resolve o nome para o ID do parlamentar, roda o pipeline completo (ingestão de 3 APIs, transformação em 3 camadas, modelagem dimensional), Dona marli executa as analises, e gera o relatório no terminal com tabelas formatadas, alertas e classificação de risco. As aspas são opcionais.

Dona Creuza com os conectores de ingestão possuem **retry automático** para erros temporários (timeout e respostas 5xx) no `CamaraDeputadosConnector` (3 tentativas, 5s de espera entre elas).

O relatório inclui:
- Painel do investigado (nome, partido, UF)
- Resumo financeiro (total de despesas, média, fornecedores únicos)
- Top 10 fornecedores por valor recebido
- Nível de risco (score baseado em alertas acumulados)
- Tabela de alertas detectados com código, descrição e detalhe

---

## Stack técnica

| Componente | Tecnologia | Função |
|---|---|---|
| Linguagem | Python 3.11+ | Única linguagem do projeto |
| HTTP Client | httpx | Requisições async às APIs |
| Armazenamento | Parquet (filesystem local) | Formato colunar para as 3 camadas |
| Motor analítico | DuckDB | SQL direto nos Parquets |
| Processamento distribuído | Apache Spark | Transformações em larga escala nas camadas Silver e Gold |
| CLI | Typer | Interface de linha de comando |
| Formatação | Rich | Tabelas e painéis no terminal |
| Logging | structlog | Logs estruturados |
| Configuração | Pydantic Settings | Variáveis de ambiente tipadas |

---

## Instalação

```bash
git clone https://github.com/yurivski/tias-do-zap.git
cd tias-do-zap
uv venv
source .venv/bin/activate
uv pip install -e .
```

Crie um arquivo `.env` na raiz:

```bash
DATA_DIR=data
TRANSPARENCIA_API_KEY=sua_chave_aqui
LOG_LEVEL=INFO
```

A chave do Portal da Transparência é obtida em [portaldatransparencia.gov.br/api-de-dados/cadastrar-email](https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email). As demais APIs não requerem autenticação.

---

## Logs

A função `setup_file_logging` redireciona todos os logs para a pasta `logs/` com nome de arquivo baseado no comando e timestamp (ex: `logs/investigate_2026-04-07_143021.log`). Cada execução gera um arquivo próprio, mantendo o histórico completo de runs anteriores.

```
logs/
├── investigate_2026-04-07_143021.log   # execução atual
├── investigate_2026-04-06_091530.log   # execução anterior
└── ...
```

O nível de log é controlado pela variável `LOG_LEVEL` no `.env` (padrão: `INFO`). Para depuração, use `LOG_LEVEL=DEBUG`.

---

## Uso rápido

```bash
# Investigação completa de um deputado
Tias_do_Zap investigate "Larapio" --ano 2024

# Ou por ID
Tias_do_Zap investigate 131313 --ano 2024

# Pipeline passo a passo
Tias_do_Zap ingest deputados --legislatura 57
Tias_do_Zap ingest ceap 131313 --ano 2024
Tias_do_Zap ingest fornecedores
Tias_do_Zap ingest emendas "LARAPIO SILVA" --ano 2024
Tias_do_Zap transform silver
Tias_do_Zap transform gold
```

A lista completa de comandos, variações e fluxo de execução está em [lourdes_documenta/commands.md](lourdes_documenta/commands.md).

---

## Arquitetura Medallion

Dona Shirley organiza os dados em três camadas, seguindo o padrão Medallion:

```
APIs Públicas ──> [BRONZE] ──> [SILVER] ──> [GOLD] ──> CLI / Relatórios
                   JSON         Parquet      Star Schema    Alertas
                   bruto        limpo        dimensional    investigação
```

**Bronze** armazena o JSON exatamente como veio da API. Nunca é alterado.

**Silver** transforma os JSONs em Parquet com schema definido: colunas renomeadas para snake_case, tipos convertidos (timestamps, floats), strings vazias substituídas por null, duplicatas removidas.

**Gold** modela os dados em Star Schema com tabelas fato e dimensão, usando surrogate keys para JOINs performáticos.

Detalhes da arquitetura, schemas de cada camada e o fluxo de transformação estão em [lourdes_documenta/architecture.md](lourdes_documenta/architecture.md).

---

## Star Schema

**Dimensões** (contexto descritivo):
- `dim_parlamentar` — parlamentares com nome, partido, UF, legislatura
- `dim_fornecedor` — empresas/pessoas que recebem reembolsos
- `dim_categoria_despesa` — tipos de despesa (combustível, escritório, etc.)

**Fato** (eventos com métricas):
- `fact_despesas_ceap` — cada reembolso individual com valor, data, e chaves para as dimensões

Cada linha da tabela fato aponta para as dimensões via surrogate keys (inteiros sequenciais). Uma query como "quanto o parlamentar X gastou com fornecedores criados há menos de 12 meses" é um JOIN entre a fato, dim_parlamentar e os dados de CNPJ da Silver.

A explicação completa de PKs, FKs, surrogate keys e como os JOINs funcionam está em [lourdes_documenta/architecture.md](lourdes_documenta/architecture.md).

---

## Sistema de alertas

O sistema detecta anomalias usando regras determinísticas, cada alerta é uma operação matemática verificável.

| Código | Alerta | Lógica |
|---|---|---|
| SPD-001 | Gasto concentrado em fornecedor | Mais de 40% do total CEAP em um único CNPJ/CPF |
| SPD-002 | Nota fiscal acima do limite estatístico | Valor > média + 3 desvios padrão |
| SUP-001 | Fornecedor recém-criado | Empresa aberta < 12 meses antes da primeira despesa |
| SUP-002 | Capital social incompatível | Total recebido > capital social declarado |
| SUP-003 | Fornecedor no CEIS | Empresa consta no Cadastro de Empresas Inidôneas |
| SUP-004 | Fornecedor no CNEP | Empresa consta no Cadastro de Empresas Punidas |

Cada alerta tem um score. A soma dos scores define a classificação de risco:

| Faixa | Classificação |
|---|---|
| 0–3 | BAIXO |
| 4–7 | MÉDIO |
| 8–12 | ALTO |
| 13+ | CRÍTICO |

As fórmulas matemáticas e a implementação de cada regra estão em [lourdes_documenta/architecture.md](lourdes_documenta/architecture.md).

---

## Documentação detalhada

| Documento | Conteúdo |
|---|---|
| [lourdes_documenta/commands.md](lourdes_documenta/commands.md) | Comandos disponíveis, ordem de execução e fluxo |
| [lourdes_documenta/apis.md](lourdes_documenta/apis.md) | APIs e endpoints em uso no projeto |
| [lourdes_documenta/architecture.md](lourdes_documenta/architecture.md) | Arquitetura, schemas, Star Schema e fórmulas |

---

## Licença

[MIT License](LICENSE).
