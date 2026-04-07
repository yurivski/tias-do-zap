import duckdb
from pathlib import Path

def search_deputado_by_name(silver_dir: Path, nome: str) -> list[dict]:
    
    deputados_path = silver_dir / "deputados.parquet"

    if not deputados_path.exists():
        return []

    con = duckdb.connect()

    # Busca por nome parcial, sem diferenciar maiúsculas/ minúsculas
    results = con.execute(f"""
        SELECT
            deputado_id,
            nome_parlamentar,
            sigla_partido,
            sigla_uf
        FROM '{deputados_path}'
        WHERE LOWER(nome_parlamentar) LIKE LOWER('%{nome}%')
        ORDER BY nome_parlamentar
    """).fetchall()

    con.close()

    return [
        {
            "id": row[0],
            "nome": row[1],
            "partido": row[2],
            "uf": row[3],
        }
        for row in results
    ]