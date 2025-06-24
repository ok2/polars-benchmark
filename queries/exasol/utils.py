from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

import pyexasol

if TYPE_CHECKING:
    import pandas as pd

from queries.common_utils import check_query_result_pd, run_query_generic
from settings import Settings

settings = Settings()

_connection: pyexasol.ExaConnection | None = None
_queries: dict[int, str] | None = None


def _load_queries() -> dict[int, str]:
    """Parse the bundled `queries.txt` file.

    Return a mapping of query number to SQL.
    """
    path = Path(__file__).parent / "queries" / "queries.txt"
    lines = path.read_text().splitlines()
    pattern = re.compile(r"\* TPC-H Query (\d+) 0 \*")
    queries: dict[int, str] = {}

    i = 0
    while i < len(lines):
        m = pattern.match(lines[i].strip())
        if m:
            qnum = int(m.group(1))
            i += 1
            # skip preamble lines
            while i < len(lines):
                line = lines[i].strip()
                if not line or line.startswith(("--", "*")):
                    i += 1
                    continue
                break
            query_lines = []
            while i < len(lines):
                line_text = lines[i].rstrip()
                query_lines.append(line_text)
                if line_text.endswith(";"):
                    i += 1
                    break
                i += 1
            queries[qnum] = "\n".join(query_lines)
        else:
            i += 1

    return queries


def get_sql_query(query_number: int) -> str:
    """Return the SQL for the given query number parsed from `queries.txt`."""
    global _queries
    if _queries is None:
        _queries = _load_queries()
    return _queries[query_number]


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
