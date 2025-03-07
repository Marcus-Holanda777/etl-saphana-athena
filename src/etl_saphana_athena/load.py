from sqlalchemy import inspect, create_engine, URL
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.engine.base import Engine
from etl_saphana_athena.config import load_config
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import tempfile

CHUNK = 100_000


def do_connect() -> Engine:
    config = load_config()
    url = URL.create("hanna", **config)
    return create_engine(url)


def get_columns(con: Engine, table_name: str, schema: str) -> dict:
    inspetor = inspect(con)

    try:
        response = inspetor.get_columns(table_name=table_name, schema=schema)
    except NoSuchTableError:
        raise ValueError("Table nao existe !")
    else:
        return {row["name"]: row["type"] for row in response}


def pandas_lotes(table_name: str, schema: str):
    engine = do_connect()
    count = 0

    with engine.begin() as con:
        for chunk in pd.read_sql_table(
            table_name, con=con, schema=schema, chunksize=CHUNK
        ):
            count += 1
            yield count, chunk


def write_parquet(table_name: str, schema: str) -> None:
    gen_dataframe = pandas_lotes(table_name, schema)

    with tempfile.NamedTemporaryFile(
        prefix="export_", suffix=".parquet", delete=False
    ) as f:
        schema = None
        for p, df in gen_dataframe:
            tbl = pa.Table.from_pandas(df, preserve_index=False)
            if schema is None:
                schema = tbl.schema

            if p == 1:
                writer = pq.ParquetWriter(f, schema=schema, compression="zstd")
            writer.write_table(tbl, row_group_size=CHUNK)

        writer.close()