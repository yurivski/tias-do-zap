import os
import asyncio
import typer
import logging
import structlog

from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F
from grupinho.dulce_config.config import get_settings
from grupinho.dulce_config.logging import setup_logging, setup_file_logging

from grupinho.creuza_conecta.camara_ceap import CamaraCeapConnector
from grupinho.creuza_conecta.camara_deputados import CamaraDeputadosConnector
from grupinho.creuza_conecta.transparencia_emendas import TransparenciaEmendasConnector
from grupinho.creuza_conecta.transparencia_contratos import TransparenciaContratosConnector
from grupinho.creuza_conecta.transparencia_servidores import TransparenciaServidoresConnector

from grupinho.shirley_transforma.silver_spark.ceap_transformer import CeapSparkTransformer
from grupinho.shirley_transforma.silver_spark.cnpj_transformer import CNPJSparkTransformer
from grupinho.shirley_transforma.silver_spark.emendas_transformer import EmendasSparkTransformer
from grupinho.shirley_transforma.silver_spark.deputados_transformer import DeputadosSparkTransformer

from grupinho.shirley_transforma.gold_spark.fact_builders import build_fact_despesas_ceap
from grupinho.shirley_transforma.gold_spark.dim_builders import (
    build_dim_parlamentar,
    build_dim_fornecedor,
    build_dim_categoria_despesa,
)

from grupinho.marli_analisa_spark.despesa_analise import SpendingSparkAnalyzer
from grupinho.marli_analisa_spark.fornecedor_analise import SupplierSparkAnalyzer
from grupinho.marli_analisa_spark.sancoes_analise import SanctionsSparkAnalyzer
from grupinho.marlene_pipeline.enriquecimento import enrich_fornecedores
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

os.environ["SPARK_NO_DAEMONIZE"] = "true"
os.environ["PYSPARK_PYTHON"] = "python"

# Silencia os logs Ivy/Hadoop que aparecem no terminal (depois de: "WARNING[...]") antes do import do Spark
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

def _get_spark() -> SparkSession:
    # Aponta direto para os JARs do Delta já baixados, sem acionar o Ivy
    ivy_jars = Path.home() / ".ivy2.5.2" / "jars"
    delta_jars = ",".join(str(j) for j in ivy_jars.glob("io.delta_*"))

    builder = (
        SparkSession.builder
        .appName("grupinho")
        .master("local[*]")
        .config("spark.jars", delta_jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# Inicializa o app Typer
app = typer.Typer(
    name="Tias do Zap",
    help="Ferramenta Open Source de inteligência e auditoria para dados abertos do Brasil.",
    no_args_is_help=True,
    )

# Sub-comandos agrupados
ingest_app = typer.Typer(help="Comandos de ingestão de dados (camada Bronze).")
transform_app = typer.Typer(help="Comandos de transformação de dados (camadas Silver/Gold).")

app.add_typer(ingest_app, name="ingest")
app.add_typer(transform_app, name="transform")

# Comandos de Ingestão
@ingest_app.command("ceap")
def ingest_ceap(
    deputado_id: int = typer.Argument(..., help="ID do deputado na API da Câmara."),
    ano: int = typer.Option(None, help="Filtrar por ano"),
):
    """Ingere dados de despesas CEAP para um parlamentar específico."""

    settings = get_settings()
    setup_logging(settings.log_level)
    logger = structlog.get_logger()

    connector = CamaraCeapConnector()

    # Monta os parâmetros da requisição
    params = {"deputado_id": deputado_id}
    if ano:
        params["ano"] = ano

    logger.info("ingestion_start", connector=connector.name, **params)

    # Executa o fetch async dentro do CLI sincrono
    async def _run():
        data = await connector.fetch_all_pages(**params)
        if not connector.validate(data):
            logger.error("validation_failed", connector=connector.name)
            raise typer.Exit(code=1)

        records = len(data.get("dados", []))
        path = connector.save_bronze(data, settings.bronze_dir)
        logger.info(
            "ingestion_complete",
            connector=connector.name,
            records=records,
            path=str(path)
        )

    asyncio.run(_run())

@ingest_app.command("deputados")
def ingest_deputados(
    legislatura: int = typer.Option(57, help="Número da legislatura (57 = atual)."),
):
    """Ingere dados cadastrais de parlamentar da API da Câmara."""

    settings = get_settings()
    setup_logging(settings.log_level)
    logger = structlog.get_logger()

    connector = CamaraDeputadosConnector()
    logger.info("ingestion_start", connector=connector.name, legislatura=legislatura)

    async def _run():
        data = await connector.fetch_all_pages(idLegislatura=legislatura)
        if not connector.validate(data):
            logger.error("validation_failed", connector=connector.name)
            raise typer.Exit(code=1)

        records = len(data.get("dados", []))
        path = connector.save_bronze(data, settings.bronze_dir)
        logger.info(
            "ingestion_complete",
            connector=connector.name,
            records=records,
            path=str(path)
        )

    asyncio.run(_run())

@ingest_app.command("fornecedores")
def ingest_fornecedores(
    delay: float = typer.Option(1.0, help="Segundos entre requisições (rate limit)"),
):
    """Enriquece dados de fornecedores buscando detalhes de CNPJ na BrasilAPI."""

    settings = get_settings()
    setup_logging(settings.log_level)

    async def _run():
        count = await enrich_fornecedores(
            settings.silver_dir,
            settings.bronze_dir,
            delay=delay
        )
        typer.echo(f"   Fornecedores enriquecidos: {count}\n")

    asyncio.run(_run())

@ingest_app.command("emendas")
def ingest_emendas(
    nome_autor: str = typer.Argument(..., help="Nome do parlamentar autor das emendas."),
    ano: int = typer.Option(None, help="Filtrar por ano."),
):
    """Ingere dados de emendas parlamentares do Portal da Transparência."""

    settings = get_settings()
    setup_logging(settings.log_level)
    logger = structlog.get_logger()

    connector = TransparenciaEmendasConnector()
    logger.info("ingestion_start", connector=connector.name, autor=nome_autor)

    async def _run():
        params = {"nomeAutor": nome_autor}
        if ano:
            params["ano"] = ano

        data = await connector.fetch(**params)
        if not connector.validate(data):
            logger.error("validation_failed", connector=connector.name)
            raise typer.Exit(code=1)

        records = len(data)
        path = connector.save_bronze(data, settings.bronze_dir)
        logger.info("ingestion_complete", connector=connector.name,
                     records=records, path=str(path))

    asyncio.run(_run())

# Comandos de Transformação
@transform_app.command("silver")
def transform_silver():
    """Transforma dados Bronze em tabelas Delta Silver."""

    settings = get_settings()
    setup_logging(settings.log_level)
    logger = structlog.get_logger()

    # Obtém a SparkSession para os transformers Spark
    spark = _get_spark()

    # Lista de transformers registrados, cada um lê da Bronze e escreve Delta na Silver
    transformers = [
        CeapSparkTransformer(spark),
        DeputadosSparkTransformer(spark),
        CNPJSparkTransformer(spark),
        EmendasSparkTransformer(spark),
    ]

    for transformer in transformers:
        logger.info("transform_start", transformer=transformer.name)
        try:
            path = transformer.transform(settings.bronze_dir, settings.silver_dir)
            logger.info(
                "transform_complete",
                transformer=transformer.name,
                path=str(path)
            )

        except ValueError as e:
            # Se não tem dados na Bronze para esse transformer, avisa e continua
            logger.warning(
                "transform_skipped",
                transformer=transformer.name,
                reason=str(e)
            )

@transform_app.command("gold")
def transform_gold():
    """Constrói o Star Schema Gold a partir dos dados Silver."""
    
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = structlog.get_logger()

    # Obtém a SparkSession compartilhada para todos os builders
    spark = _get_spark()

    silver = settings.silver_dir
    gold = settings.gold_dir

    # Constrói dimensões primeiro
    logger.info("building_dimensions")

    path = build_dim_parlamentar(spark, silver, gold)
    logger.info("dim_built", table="dim_parlamentar", path=str(path))

    path = build_dim_fornecedor(spark, silver, gold)
    logger.info("dim_built", table="dim_fornecedor", path=str(path))

    path = build_dim_categoria_despesa(spark, silver, gold)
    logger.info("dim_built", table="dim_categoria_despesa", path=str(path))

    # Depois constrói a tabela fato
    logger.info("building_fact")
    path = build_fact_despesas_ceap(spark, silver, gold)
    logger.info("fact_built", table="fact_despesas_ceap", path=str(path))
    logger.info("gold_complete")

@app.command("check-servidor")
def check_servidor(
    cpf: str = typer.Argument(..., help="CPF completo do servidor (11 dígitos)."),
):
    """Verifica se um CPF pertence a um servidor público federal."""

    settings = get_settings()
    setup_logging(settings.log_level)
    console = Console()

    connector = TransparenciaServidoresConnector()

    async def _run():
        data = await connector.fetch(cpf=cpf)

        if not data:
            console.print(f"\n[yellow]CPF {cpf} nao consta na base de servidores federais.[/yellow]\n")
            return

        console.print(f"\n[red bold]SERVIDOR PUBLICO ENCONTRADO[/red bold]\n")

        for servidor in data:
            console.print(Panel(
                f"Nome: {servidor.get('nome', '')}\n"
                f"CPF: {servidor.get('cpf', '')}\n"
                f"Orgao Lotacao: {servidor.get('orgaoServidorLotacao', '')}\n"
                f"Orgao Exercicio: {servidor.get('orgaoServidorExercicio', '')}\n"
                f"Cargo: {servidor.get('cargo', '')}\n"
                f"Funcao: {servidor.get('funcao', '')}",
                title="[bold white]Servidor[/bold white]",
                border_style="red",
            ))

    asyncio.run(_run())

@app.command("check-contrato")
def check_contrato(
    codigo_orgao: str = typer.Argument(..., help="Codigo do orgao (SIAPE)."),
    pagina: int = typer.Option(1, help="Pagina de resultados."),
):
    """Verifica contratos federais de um órgão governamental específico."""

    settings = get_settings()
    setup_logging(settings.log_level)
    console = Console()

    connector = TransparenciaContratosConnector()

    async def _run():
        data = await connector.fetch(codigo_orgao=codigo_orgao, pagina=pagina)

        if not data:
            console.print(f"\n[yellow]Nenhum contrato encontrado para o orgao {codigo_orgao}.[/yellow]\n")
            return

        table = Table(title=f"Contratos - Orgao {codigo_orgao}", border_style="blue")
        table.add_column("Numero", style="cyan")
        table.add_column("Fornecedor", style="white", ratio=3)
        table.add_column("Objeto", style="dim", ratio=4)
        table.add_column("Valor (R$)", justify="right", style="green")

        # Remove contratos duplicados (API retorna registros com IDs diferentes mas conteúdo igual)
        seen = set()
        unique_data = []
        for contrato in data:
            key = contrato.get("numero", "")
            if key not in seen:
                seen.add(key)
                unique_data.append(contrato)
        data = unique_data

        for contrato in data:
            fornecedor = contrato.get("fornecedor", {}).get("nome", "")
            valor = contrato.get("valorInicialCompra", 0)
            table.add_row(
                str(contrato.get("numero", "")),
                fornecedor,
                (contrato.get("objeto", "") or "")[:60],
                f"{valor:,.2f}" if valor else "—",
            )

        console.print(table)

    asyncio.run(_run())

@app.command("investigate")
def investigate(
    query: str = typer.Argument(..., help="ID ou nome do deputado."),
    ano: int = typer.Option(None, help="Filtrar despesas por ano."),
    legislatura: int = typer.Option(57, help="Número da legislatura."),
    skip_ingest: bool = typer.Option(False, help="Pular ingestão (usar dados existentes)."),
):
    """Investiga um parlamentar: ingere dados, constrói lakehouse, executa detecção de anomalias."""

    log_file = setup_file_logging("investigate")
    console = Console()
    settings = get_settings()
    setup_logging(settings.log_level)

    # Inicializa a SparkSession que será reutilizada em toda a investigação via getOrCreate
    spark = _get_spark()

    # Etapa de ingestão e tranformação
    if not skip_ingest:
        console.print("\n[bold cyan]════════════════════ TIAS DO ZAP - Investigation ════════════════════[/bold cyan]\n", justify="center")

        console.print("[yellow]   [1/6] Dona Zilda está ingerindo dados de parlamentares...[/yellow]")
        ingest_deputados(legislatura=legislatura)

        console.print("[yellow]   [2/6] Dona Shirley realiza a primeira trasnformação de dados...[/yellow]")
        transform_silver()
    else:
        console.print("\n[bold cyan]════════════════════ TIAS DO ZAP - Investigation ════════════════════[/bold cyan]\n", justify="center")
        console.print("[yellow]   [INFO] Pulando ingestão, Dona Shirley já tem as informações (usando dados em cache)...[/yellow]")

    # Query: pode ser ID numérico ou nome
    if query.isdigit():
        deputado_id = int(query)
    else:
        # Busca por nome parcial na tabela Delta de deputados da Silver
        df_dep = spark.read.format("delta").load(str(settings.silver_dir / "deputados"))
        matches_rows = (
            df_dep
            .filter(F.lower(F.col("nome_parlamentar")).like(f"%{query.lower()}%"))
            .select("deputado_id", "nome_parlamentar", "sigla_partido", "sigla_uf")
            .orderBy("nome_parlamentar")
            .collect()
        )
        matches = [
            {"id": r.deputado_id, "nome": r.nome_parlamentar, "partido": r.sigla_partido, "uf": r.sigla_uf}
            for r in matches_rows
        ]

        if not matches:
            console.print(f"[red]   Nenhum parlamentar encontrado com o nome '{query}'.[/red]")
            raise typer.Exit(code=1)

        if len(matches) == 1:
            deputado_id = matches[0]["id"]
            console.print(f"   Parlamentar encontrado: {matches[0]['nome']} ({matches[0]['partido']}-{matches[0]['uf']})\n")
        else:
            console.print(f"[yellow]Encontrados {len(matches)} parlamentares:[/yellow]\n")
            for i, m in enumerate(matches, 1):
                console.print(f"  {i}. {m['nome']} ({m['partido']}-{m['uf']}) [dim]ID: {m['id']}[/dim]")
            console.print("")
            choice = typer.prompt("Selecione o número do parlamentar", type=int)
            if choice < 1 or choice > len(matches):
                console.print("[red]Opção inválida.[/red]")
                raise typer.Exit(code=1)
            deputado_id = matches[choice - 1]["id"]

    # Continua o pipeline com o deputado_id resolvido
    if not skip_ingest:
        console.print(f"[yellow]   [3/6] Dona Zilda está ingerindo dados do CEAP...[/yellow]")
        ingest_ceap(deputado_id=deputado_id, ano=ano)

        # Transforma as despesas CEAP na Silver antes de enriquecer fornecedores,
        # pois o enrich lê os CNPJs a partir dos dados Silver já transformados
        console.print(f"[yellow]   [4/6] Dona Shirley está transformando despesas e Dona Marlene enriquecendo fornecedores...[/yellow]")
        transform_silver()
        ingest_fornecedores(delay=1.5)

        console.print(f"[yellow]   [5/6] Dona Marli está processando dados de fornecedores...[/yellow]")
        transform_silver()

        console.print(f"[yellow]   [6/6] Dona Shirley agora está construindo o star schema gold...[/yellow]")
        transform_gold()

    # Análise de anomalias, instancia os analyzers com a SparkSession
    console.print("\n[yellow]   [INFO] 1/2 ≫[/yellow] Dona Neide está executando a detecção de anomalias...")
    spending_analyzer = SpendingSparkAnalyzer(spark)
    supplier_analyzer = SupplierSparkAnalyzer(spark)
    sanctions_analyzer = SanctionsSparkAnalyzer(spark)

    alerts = []
    alerts.extend(spending_analyzer.analyze(settings.gold_dir, deputado_id=deputado_id))
    alerts.extend(supplier_analyzer.analyze(settings.gold_dir, deputado_id=deputado_id))
    alerts.extend(sanctions_analyzer.analyze(settings.gold_dir, deputado_id=deputado_id))

    # Relatório
    console.print("[yellow]   [INFO] 2/2 ≫[/yellow] Dona Fátima está gerando relatório para a Dona Cida...\n")

    gold = settings.gold_dir
    silver = settings.silver_dir

    # Lê a dimensão parlamentar da Gold para exibir os dados do investigado
    dim_parlamentar = spark.read.format("delta").load(str(gold / "dim_parlamentar"))
    dep_row = (
        dim_parlamentar
        .filter(F.col("deputado_id") == deputado_id)
        .select("nome_parlamentar", "sigla_partido", "sigla_uf")
        .first()
    )

    if dep_row:
        console.print(Panel(
            f"[bold]{dep_row['nome_parlamentar']}[/bold]\n"
            f"Partido: {dep_row['sigla_partido']} | UF: {dep_row['sigla_uf']}\n"
            f"ID: {deputado_id}",
            title="[bold white]Dona Cida fornece - Investigado[/bold white]",
            border_style="cyan",
        ))

    # Lê a tabela fato e filtra pelo parlamentar para o resumo financeiro
    fact = spark.read.format("delta").load(str(gold / "fact_despesas_ceap"))
    keys = dim_parlamentar.filter(F.col("deputado_id") == deputado_id).select("parlamentar_key")
    fact_dep = fact.join(keys, on="parlamentar_key", how="inner")

    resumo = fact_dep.agg(
        F.count("*").alias("total_despesas"),
        F.round(F.sum("valor_liquido"), 2).alias("total_gasto"),
        F.round(F.avg("valor_liquido"), 2).alias("media_gasto"),
        F.countDistinct("fornecedor_key").alias("total_fornecedores"),
    ).first()

    if resumo and resumo["total_despesas"] is not None and resumo["total_gasto"] is not None:
        console.print(Panel(
            f"Total de despesas: {resumo['total_despesas']}\n"
            f"Valor total: R$ {resumo['total_gasto']:,.2f}\n"
            f"Média por despesa: R$ {resumo['media_gasto']:,.2f}\n"
            f"Fornecedores únicos: {resumo['total_fornecedores']}",
            title="[bold white]Dona Marli fornece - Resumo Financeiro[/bold white]",
            border_style="green",
        ))
    else:
        console.print(Panel(
            "Nenhuma despesa encontrada para este parlamentar.\n"
            f"Execute primeiro: grupinho ingest ceap {deputado_id} --ano {ano}",
            title="[bold white]Dona Marli fornece - Resumo Financeiro[/bold white]",
            border_style="yellow",
        ))

    # Top 10 fornecedores por valor total recebido
    dim_fornecedor = spark.read.format("delta").load(str(gold / "dim_fornecedor"))
    top_forn = (
        fact_dep
        .join(dim_fornecedor.select("fornecedor_key", "nome_fornecedor", "cnpj_cpf_fornecedor"), on="fornecedor_key")
        .groupBy("nome_fornecedor", "cnpj_cpf_fornecedor")
        .agg(F.count("*").alias("qtd"), F.round(F.sum("valor_liquido"), 2).alias("total"))
        .orderBy(F.col("total").desc())
        .limit(10)
        .collect()
    )

    if top_forn:
        table = Table(title="Top Fornecedores", border_style="blue", expand=True)
        table.add_column("Fornecedor", style="white", ratio=4, overflow="ellipsis", no_wrap=True)
        table.add_column("CNPJ/CPF", style="dim", ratio=3)
        table.add_column("Notas", justify="right", ratio=1)
        table.add_column("Total (R$)", justify="right", style="green", ratio=2)

        for row in top_forn:
            table.add_row(row["nome_fornecedor"], row["cnpj_cpf_fornecedor"], str(row["qtd"]), f"{row['total']:,.2f}")

        console.print(table)

    # Sócios dos fornecedores, disponível apenas se o enriquecimento CNPJ foi executado.
    # A presença do _delta_log indica que a tabela Delta foi gravada na Silver.
    socios_path = silver / "cnpj_socios"
    if (socios_path / "_delta_log").exists():
        df_socios = spark.read.format("delta").load(str(socios_path))
        df_empresas = spark.read.format("delta").load(str(silver / "cnpj_empresas"))

        # Restringe aos CNPJs dos fornecedores deste deputado (only 14+ chars = CNPJs, não CPFs)
        cnpjs_do_deputado = (
            fact_dep
            .join(dim_fornecedor.select("fornecedor_key", "cnpj_cpf_fornecedor"), on="fornecedor_key")
            .filter(F.length(F.col("cnpj_cpf_fornecedor")) >= 14)
            .select("cnpj_cpf_fornecedor")
            .distinct()
        )

        socios_data = (
            df_socios
            .join(cnpjs_do_deputado, df_socios["cnpj"] == cnpjs_do_deputado["cnpj_cpf_fornecedor"])
            .join(df_empresas.select("cnpj", "razao_social"), on="cnpj")
            .select("cnpj", "nome_socio", "qualificacao_socio", "razao_social")
            .orderBy("razao_social", "nome_socio")
            .limit(20)
            .collect()
        )

        if socios_data:
            socios_table = Table(title="Socios dos Fornecedores", border_style="cyan")
            socios_table.add_column("Empresa", style="white", ratio=3)
            socios_table.add_column("Socio", style="white", ratio=3)
            socios_table.add_column("Qualificacao", style="dim", ratio=2)

            for row in socios_data:
                socios_table.add_row(row["razao_social"], row["nome_socio"], row["qualificacao_socio"] or "")

            console.print(socios_table)
            console.print("[dim]Dona Marli informa: para verificar se um socio e servidor publico: grupinho check-servidor --cpf {CPF_COMPLETO}[/dim]\n")

    # Emendas parlamentares, disponível apenas se o comando ingest emendas foi executado.
    # A presença do _delta_log indica que a tabela Delta foi gravada na Silver.
    emendas_path = silver / "emendas"
    if (emendas_path / "_delta_log").exists():
        nome_dep = dep_row["nome_parlamentar"] if dep_row else ""
        df_emendas = spark.read.format("delta").load(str(emendas_path))

        emendas_data = (
            df_emendas
            .filter(
                F.lower(F.col("nome_autor")).like(f"%{nome_dep.lower()}%") &
                (F.col("valor_pago") > 0)
            )
            .select("localidade_gasto", "funcao", "valor_pago")
            .orderBy(F.col("valor_pago").desc())
            .collect()
        )

        if emendas_data:
            total_emendas = sum(r["valor_pago"] for r in emendas_data)

            emendas_table = Table(title="Emendas Parlamentares", border_style="magenta", expand=True)
            emendas_table.add_column("Localidade", style="white", ratio=4, overflow="ellipsis", no_wrap=True)
            emendas_table.add_column("Função", style="white", ratio=3)
            emendas_table.add_column("Valor Pago (R$)", justify="right", style="green", ratio=2)

            for row in emendas_data:
                emendas_table.add_row(row["localidade_gasto"], row["funcao"], f"{row['valor_pago']:,.2f}")

            # Linha de total
            emendas_table.add_row("", "[bold]TOTAL[/bold]", f"[bold]{total_emendas:,.2f}[/bold]")

            console.print(emendas_table)

    # Alertas de anomalias detectados pelos analyzers
    if alerts:
        # Calcula o score total somando o peso de cada alerta
        total_score = sum(a.score for a in alerts)
        if total_score <= 3:
            risk_color = "green"
            risk_label = "BAIXO"
        elif total_score <= 7:
            risk_color = "yellow"
            risk_label = "MÉDIO"
        elif total_score <= 12:
            risk_color = "red"
            risk_label = "ALTO"
        else:
            risk_color = "bold red"
            risk_label = "CRÍTICO"

        console.print(Panel(
            f"Alertas encontrados: {len(alerts)}\n"
            f"Score total: {total_score}\n"
            f"Classificação: [{risk_color}]{risk_label}[/{risk_color}]",
            title="[bold white]Dona Shirley informa - Nível de Risco[/bold white]",
            border_style=risk_color,
        ))

        # Tabela de alertas
        alert_table = Table(title="Alertas Detectados", border_style="red", expand=True)
        alert_table.add_column("Código", style="red bold", ratio=1)
        alert_table.add_column("Descrição", style="white", ratio=3)
        alert_table.add_column("Score", justify="right", ratio=1)
        alert_table.add_column("Detalhe", style="dim", ratio=4, overflow="ellipsis", no_wrap=True)

        for alert in alerts:
            # Monta detalhe resumido com base no código do alerta
            if alert.code == "SPD-001":
                detalhe = f"{alert.details['fornecedor']} ({alert.details['percentual']}%)"
            elif alert.code == "SPD-002":
                detalhe = f"R${alert.details['valor']:,.2f} - {alert.details['fornecedor']}"
            elif alert.code == "SUP-001":
                detalhe = f"{alert.details['fornecedor']} ({alert.details['meses_diferenca']} meses)"
            elif alert.code == "SUP-002":
                detalhe = f"{alert.details['fornecedor']} (R${alert.details['total_recebido']:,.2f} / capital R${alert.details['capital_social']:,.2f})"
            elif alert.code == "SUP-003":
                detalhe = f"{alert.details['fornecedor']} - CEIS ({alert.details['tipo_sancao']})"
            elif alert.code == "SUP-004":
                detalhe = f"{alert.details['fornecedor']} - CNEP ({alert.details['tipo_sancao']})"
            else:
                detalhe = str(alert.details)

            alert_table.add_row(alert.code, alert.description, str(alert.score), detalhe)

        console.print(alert_table)
    else:
        console.print(Panel(
            "Nenhuma anomalia detectada.",
            title="[bold white]Alertas[/bold white]",
            border_style="green",
        ))

    console.print(f"\n[bold cyan] Investigation Complete - Ótimo trabalho das Tias e suas Fontes [/bold cyan]\n", justify="center")

if __name__ == "__main__":
    app()
