from __future__ import annotations

import pandas as pd
import pyexasol

from queries.common_utils import check_query_result_pd, run_query_generic
from settings import Settings

settings = Settings()

_connection: pyexasol.ExaConnection | None = None


def get_connection() -> pyexasol.ExaConnection:
    global _connection
    if _connection is None:
        dsn = f"{settings.exasol.host}:{settings.exasol.port}"
        _connection = pyexasol.connect(
            dsn=dsn,
            user=settings.exasol.user,
            password=settings.exasol.password,
            schema=settings.exasol.schema,
        )
    return _connection


def get_line_item_ds() -> str:
    return "lineitem"


def get_orders_ds() -> str:
    return "orders"


def get_customer_ds() -> str:
    return "customer"


def get_region_ds() -> str:
    return "region"


def get_nation_ds() -> str:
    return "nation"


def get_supplier_ds() -> str:
    return "supplier"


def get_part_ds() -> str:
    return "part"


def get_part_supp_ds() -> str:
    return "partsupp"


def run_query(query_number: int, query: str) -> None:
    conn = get_connection()

    def execute() -> pd.DataFrame:
        return conn.export_to_pandas(query)

    run_query_generic(
        execute,
        query_number,
        "exasol",
        library_version=pyexasol.__version__,
        query_checker=check_query_result_pd,
    )
