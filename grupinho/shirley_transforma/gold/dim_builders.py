import duckdb
import pandas as pd
from pathlib import Path

def build_dim_parlamentar(silver_dir: Path, gold_dir: Path) -> Path:
    # Lê o parquet de parlamentares da silver
    deputados_path = silver_dir / "deputados.parquet"
    df = pd.read_parquet(deputados_path)

    # Seleciona e renomeia colunas pra a dimensão
    dim = df[[
        "deputado_id",
        "nome_parlamentar",
        "sigla_partido",
        "sigla_uf",
        "id_legislatura",
        "email"
    ]].copy()

    # Adiciona surrogate key
    dim.insert(0, "parlamentar_key", range(1, len(dim) + 1))

    # Remove duplicatas pelo deputado_id
    dim = dim.drop_duplicates(subset=["deputado_id"], keep="first")

    # Persiste na Gold
    gold_dir.mkdir(parents=True, exist_ok=True)
    output_path = gold_dir / "dim_parlamentar.parquet"
    dim.to_parquet(output_path, index=False, engine="pyarrow")

    return output_path

def build_dim_fornecedor(silver_dir: Path, gold_dir: Path) -> Path:
    # Lê o Parquet de despesas da silver
    ceap_path = silver_dir / "ceap_despesas.parquet"
    df = pd.read_parquet(ceap_path)

    # Extrai fornecedores únicos
    dim = df[[
        "cnpj_cpf_fornecedor",
        "nome_fornecedor"
    ]].drop_duplicates(
        subset=["cnpj_cpf_fornecedor"],
        keep="first"
    ).copy()

    # Adiciona surrogate key
    dim.insert(0, "fornecedor_key", range(1, len(dim) + 1))

    # Persiste na gold
    gold_dir.mkdir(parents=True, exist_ok=True)
    output_path = gold_dir / "dim_fornecedor.parquet"
    dim.to_parquet(
        output_path,
        index=False,
        engine="pyarrow"
    )

    return output_path

def build_dim_categoria_despesa(silver_dir: Path, gold_dir: Path) -> Path:
    # Lê o parquet de despesas da silver
    ceap_path = silver_dir / "ceap_despesas.parquet"
    df = pd.read_parquet(ceap_path)

    # Extrai categorias únicas
    dim = df[["tipo_despesa"]].drop_duplicates().copy()

    # Adiciona surrogate key
    dim.insert(0, "categoria_key", range(1, len(dim) + 1))

    # Renomeia para o padrão da dimensão
    dim = dim.rename(columns={"tipo_despesa": "descricao_despesa"})

    # Persiste na gold
    gold_dir.mkdir(parents=True, exist_ok=True)
    output_path = gold_dir / "dim_categoria_despesa.parquet"
    dim.to_parquet(
        output_path,
        index=False,
        engine="pyarrow"
    )

    return output_path