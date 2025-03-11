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
from textual.widgets import Label
from athena_mvsh import Athena, CursorParquetDuckdb
from typing import Literal


CHUNK = 10_000

MAP_TYPES = {
    types.BIGINT: pa.int64(),
    types.INTEGER: pa.int32(),
    types.SMALLINT: pa.int16(),
    types.TINYINT: pa.int8(),
    types.BOOLEAN: pa.bool_(),
    types.VARCHAR: pa.string(),
    types.CHAR: pa.string(),
    types.NVARCHAR: pa.string(),
    types.CLOB: pa.string(),
    types.NCHAR: pa.string(),
    types.DOUBLE: pa.float64(),
    types.FLOAT: pa.float32(),
    types.REAL: pa.float64(),
    types.DECIMAL: pa.float32(),
    types.DATE: pa.date64(),
    types.TIMESTAMP: pa.timestamp("ns"),
}


def do_connect() -> Engine:
    config = load_config().get("sap")

    if config:
        url = URL.create("hana", **config)
    else:
        raise ValueError("Arquivo config nao existe !")

    return create_engine(url)


def create_stmt(con: Engine, table_name: str, schema: str) -> str:
    inspetor = inspect(con)
    response = inspetor.get_columns(table_name=table_name, schema=schema)

    columns = [
        f"TO_VARCHAR({row['name']}) AS {row['name']}"
        if isinstance(row["type"], types.CLOB)
        else f"{row['name']}"
        for row in response
    ]

    return f"""select {",".join(columns)} from {schema}.{table_name}"""


def get_columns(con: Engine, table_name: str, schema: str) -> pa.Schema:
    inspetor = inspect(con)

    try:
        response = inspetor.get_columns(table_name=table_name, schema=schema)
    except NoSuchTableError:
        raise ValueError("Tabela nao existe !")
    else:
        return pa.schema(
            [(row["name"], MAP_TYPES.get(type(row["type"]))) for row in response]
        )


def export_athena(
    file: str,
    table_name: str,
    schema: str,
    operation: Literal["replace", "append", "merge"] = "replace",
) -> None:
    if operation == "merge":
        return

    config = load_config().get("athena")
    location = config.pop("s3_dir")

    cursor = CursorParquetDuckdb(**config)

    with Athena(cursor=cursor) as client:
        client.write_table_iceberg(
            file,
            table_name=table_name,
            schema=schema,
            location=f"{location}{table_name}/",
            if_exists=operation,
        )


async def async_do_connect():
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, do_connect)


async def async_create_stmt(con: Engine, table_name: str, schema: str) -> str:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, create_stmt, con, table_name, schema)


async def async_get_columns(con: Engine, table_name: str, schema: str) -> pa.Schema:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, get_columns, con, table_name, schema)


async def async_pandas_lotes(table_name: str, schema: str):
    engine = await async_do_connect()
    dtype_arrow = await async_get_columns(engine, table_name, schema)
    stmt = await async_create_stmt(engine, table_name, schema)

    yield dtype_arrow

    with engine.begin() as con:
        for chunk in pd.read_sql(stmt, con=con, chunksize=CHUNK):
            yield chunk


async def async_export_athena(*args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, export_athena, *args)


async def write_parquet(
    table_name: str,
    schema: str,
    status: Label,
    aws_schema: str,
    aws_table_name: str,
    aws_operation: Literal["replace", "append", "merge"],
) -> None:
    gen_dataframe = async_pandas_lotes(table_name, schema)
    dtype_arrow = await gen_dataframe.__anext__()

    status.update("SAP: Tipos Arrow definido ...")

    with tempfile.NamedTemporaryFile(
        prefix="export_", suffix=".parquet", delete=False
    ) as f:
        to_pandas = partial(
            pa.Table.from_pandas, preserve_index=False, schema=dtype_arrow
        )
        loop = asyncio.get_running_loop()

        with pq.ParquetWriter(f, schema=dtype_arrow, compression="zstd") as writer:
            total = 0
            async for df in gen_dataframe:
                rows, __ = df.shape
                total += rows

                tbl = await loop.run_in_executor(None, to_pandas, df)
                await loop.run_in_executor(None, writer.write_table, tbl, CHUNK)
                status.update(f"SAP: {table_name}, {total}")

        f.close()

        status.update(f"ATHENA: init: {aws_table_name} - {aws_operation}")
        await async_export_athena(f.name, aws_table_name, aws_schema, aws_operation)
        status.update(f"ATHENA: fin: {table_name} - {total}")
