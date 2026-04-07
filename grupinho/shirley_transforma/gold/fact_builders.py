import pandas as pd
from pathlib import Path

def build_fact_despesas_ceap(silver_dir: Path, gold_dir: Path) -> Path:
    # Lê os dados da silver
    ceap_path = silver_dir / "ceap_despesas.parquet"
    df = pd.read_parquet(ceap_path)

    # Lê as dimensões da gold
    dim_parlamentar = pd.read_parquet(gold_dir / "dim_parlamentar.parquet")
    dim_fornecedor = pd.read_parquet(gold_dir / "dim_fornecedor.parquet")
    dim_categoria = pd.read_parquet(gold_dir / "dim_categoria_despesa.parquet")

    # JOIN com a dim_parlamentar, troca o "deputado_id" pela surrogate_key "parlamentar_key"
    df = df.merge(
        dim_parlamentar[["parlamentar_key", "deputado_id"]],
        on="deputado_id",
        how="left",
    )

    # JOIN com dim_fornecedor
    # Troca o cnpj_cpf_fornecedor pela surrogate key fornecedor_key
    df = df.merge(
        dim_fornecedor[["fornecedor_key", "cnpj_cpf_fornecedor"]],
        on="cnpj_cpf_fornecedor",
        how="left",
    )

    # JOIN com dim_categoria_despesa
    # troca o tipo_despesa pela surrogate key categoria_key
    df = df.merge(
        dim_categoria[["categoria_key", "descricao_despesa"]],
        left_on="tipo_despesa",
        right_on="descricao_despesa",
        how="left",
    )

    # Monta a tabela fato final
    # Seleciona apenas as colunas que pertencem à fato
    fact = df[[
        "parlamentar_key",
        "fornecedor_key",
        "categoria_key",
        "cod_documento",
        "num_documento",
        "data_documento",
        "ano_referencia",
        "mes_referencia",
        "valor_documento",
        "valor_liquido",
        "valor_glosa",
        "tipo_documento",
        "url_documento",
    ]].copy()

    # Adicionar surrogate_key da fato
    fact.insert(0, "despesa_key", range(1, len(fact) + 1))

    # Ordena por ano e mês
    fact = fact.sort_values(
        by=["ano_referencia", "mes_referencia"],
        ascending=[True, True],
    ).reset_index(drop=True)

    # Persiste na gold
    output_path = gold_dir / "fact_despesas_ceap.parquet"
    fact.to_parquet(output_path, index=False, engine="pyarrow")

    return output_path