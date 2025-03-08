from sqlalchemy import inspect, create_engine, URL
import sqlalchemy_hana.types as types
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.engine.base import Engine
from etl_saphana_athena.config import load_config
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import tempfile
import asyncio
from functools import partial


CHUNK = 100_000

MAP_TYPES = {
    types.BIGINT: "Int64",
    types.INTEGER: "Int32",
    types.SMALLINT: "Int32",
    types.TINYINT: "Int32",
    types.BOOLEAN: "bool",
    types.VARCHAR: "str",
    types.CHAR: "str",
    types.NVARCHAR: "str",
    types.NCHAR: "str",
    types.DOUBLE: "float64",
    types.FLOAT: "float64",
    types.REAL: "float64",
    types.DECIMAL: "float64",
    types.DATE: "datetime64[ns]",
    types.TIMESTAMP: "datetime64[ns]",
}


def do_connect() -> Engine:
    config = load_config().get("sap")

    if config:
        url = URL.create("hana", **config)
    else:
        raise ValueError("Arquivo config nao existe !")

    return create_engine(url)


def get_columns(con: Engine, table_name: str, schema: str) -> dict:
    inspetor = inspect(con)

    try:
        response = inspetor.get_columns(table_name=table_name, schema=schema)
    except NoSuchTableError:
        raise ValueError("Tabela nao existe !")
    else:
        return {row["name"]: MAP_TYPES.get(row["type"], "object") for row in response}


def pandas_lotes(table_name: str, schema: str):
    engine = do_connect()
    dtype = get_columns(engine, table_name, schema)
    count = 0

    stmt = f"""
    select * from {schema}.{table_name}
    """

    with engine.begin() as con:
        for chunk in pd.read_sql(
            stmt, con=con, schema=schema, chunksize=CHUNK, dtype=dtype
        ):
            count += 1
            yield count, chunk


async def write_parquet(table_name: str, schema: str) -> None:
    gen_dataframe = pandas_lotes(table_name, schema)

    with tempfile.NamedTemporaryFile(
        prefix="export_", suffix=".parquet", delete=False
    ) as f:
        schema = None
        writer = None

        to_pandas = partial(pa.Table.from_pandas, preserve_index=False)

        loop = asyncio.get_running_loop()
        for p, df in gen_dataframe:
            tbl = await loop.run_in_executor(None, to_pandas, df)
            if schema is None:
                schema = tbl.schema

            if p == 1:
                writer = pq.ParquetWriter(f, schema=schema, compression="zstd")

            await loop.run_in_executor(None, writer.write_table, tbl, CHUNK)

        if writer:
            await loop.run_in_executor(None, writer.close)
