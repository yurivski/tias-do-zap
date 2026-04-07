import time
import asyncio
import structlog
import pandas as pd

from pathlib import Path
from grupinho.dulce_config.config import get_settings
from grupinho.creuza_conecta.brasil_api_cnpj import BrasilApiCNPJConnector

logger = structlog.get_logger()

async def enrich_fornecedores(silver_dir: Path, bronze_dir: Path, delay: float = 1.0) -> int:

    # Lê os CNPJs únicos da Silver de despesas
    # Suporta tanto o formato parquet (pipeline pandas) quanto Delta (pipeline Spark)
    ceap_parquet = silver_dir / "ceap_despesas.parquet"
    ceap_delta = silver_dir / "ceap_despesas"

    if ceap_parquet.exists():
        df = pd.read_parquet(ceap_parquet)
    elif ceap_delta.exists():
        df = pd.read_parquet(ceap_delta)
    else:
        logger.error("ceap_not_found", path=str(silver_dir / "ceap_despesas"))
        return 0
    cnpjs = df["cnpj_cpf_fornecedor"].dropna().unique().tolist()

    # Filtra apenas CNPJs e ignora CPFs
    cnpjs = [c for c in cnpjs if len(str(c)) >= 14]

    logger.info("cnpjs_found", total=len(cnpjs))

    connector = BrasilApiCNPJConnector()
    success_count = 0

    for i, cnpj in enumerate(cnpjs, 1):
        logger.info("enriching", cnpj=cnpj, progress=f"{i}/{len(cnpjs)}")

        try:
            # Busca dados da empresa
            data = await connector.fetch(cnpj=cnpj)

            # Valida a resposta
            if not connector.validate(data):
                logger.warning("validation_failed", cnpj=cnpj)
                continue

            # Salva na Bronze
            connector.save_bronze(data, bronze_dir)
            success_count += 1
            logger.info(
                "enriched",
                cnpj=cnpj,
                razao_social=data.get("razao_social",
                "")
            )

        except Exception as e:
            # Se der erro (CNPJ inválido, API fora, etc), loga e continua
            logger.warning("enrich_failed", cnpj=cnpj, error=str(e))

        # Respeita o rate limit entre requisições
        if i < len(cnpjs):
            await asyncio.sleep(delay)

    logger.info("enrichment_complete", success=success_count, total=len(cnpjs))
    return success_count
