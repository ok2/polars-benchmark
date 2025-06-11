from __future__ import annotations

import re
import sys
from importlib.metadata import version
from pathlib import Path
from subprocess import run
import os
from typing import TYPE_CHECKING, Any

from linetimer import CodeTimer

import pandas as pd
import warnings
from settings import Settings

if TYPE_CHECKING:
    from collections.abc import Callable

    import pandas as pd
    import polars as pl

settings = Settings()


def get_table_path(table_name: str) -> Path:
    """Return the path to the given table."""
    ext = settings.run.io_type if settings.run.include_io else "parquet"
    return settings.dataset_base_dir / f"{table_name}.{ext}"


def log_query_timing(
    solution: str, version: str, query_number: int, time: float
) -> None:
    settings.paths.timings.mkdir(parents=True, exist_ok=True)

    with (settings.paths.timings / settings.paths.timings_filename).open("a") as f:
        if f.tell() == 0:
            f.write("solution,version,query_number,duration[s],io_type,scale_factor\n")

        line = (
            ",".join(
                [
                    solution,
                    version,
                    str(query_number),
                    str(time),
                    settings.run.io_type,
                    str(settings.scale_factor),
                ]
            )
            + "\n"
        )
        f.write(line)


def on_second_call(func: Any) -> Any:
    def helper(*args: Any, **kwargs: Any) -> Any:
        helper.calls += 1  # type: ignore[attr-defined]

        # first call is outside the function
        # this call must set the result
        if helper.calls == 1:  # type: ignore[attr-defined]
            # include IO will compute the result on the 2nd call
            if not settings.run.include_io:
                helper.result = func(*args, **kwargs)  # type: ignore[attr-defined]
            return helper.result  # type: ignore[attr-defined]

        # second call is in the query, now we set the result
        if settings.run.include_io and helper.calls == 2:  # type: ignore[attr-defined]
            helper.result = func(*args, **kwargs)  # type: ignore[attr-defined]

        return helper.result  # type: ignore[attr-defined]

    helper.calls = 0  # type: ignore[attr-defined]
    helper.result = None  # type: ignore[attr-defined]

    return helper


def execute_all(library_name: str) -> None:
    print(settings.model_dump_json())

    query_numbers = _get_query_numbers(library_name)
    total_runs = settings.run.suite_iterations

    overall_name = f"Overall execution of ALL {library_name} queries"
    if total_runs != 1:
        overall_name += f" x{total_runs}"
    with CodeTimer(name=overall_name, unit="s"):
        for run_idx in range(total_runs):
            suite_name = f"Suite {run_idx + 1}/{total_runs} execution of ALL {library_name} queries"
            with CodeTimer(name=suite_name, unit="s"):
                for i in query_numbers:
                    env = os.environ.copy()
                    env["RUN_SUITE_ITERATION"] = str(run_idx + 1)
                    run([sys.executable, "-m", f"queries.{library_name}.q{i}"], env=env)


def _get_query_numbers(library_name: str) -> list[int]:
    """Get the query numbers that are implemented for the given library."""
    query_numbers = []

    path = Path(__file__).parent / library_name
    expr = re.compile(r"q(\d+).py$")

    for file in path.iterdir():
        match = expr.search(str(file))
        if match is not None:
            query_numbers.append(int(match.group(1)))

    return sorted(query_numbers)


def run_query_generic(
    query: Callable[..., Any],
    query_number: int,
    library_name: str,
    library_version: str | None = None,
    query_checker: Callable[..., None] | None = None,
) -> None:
    """Execute a query."""
    for iter_idx in range(settings.run.iterations):
        name = f"Run {library_name} query {query_number}"
        if settings.run.suite_iterations != 1:
            name += f" [suite {settings.run.suite_iteration}/{settings.run.suite_iterations}]"
        if settings.run.iterations != 1:
            name += f" [iter {iter_idx + 1}/{settings.run.iterations}]"
        with CodeTimer(name=name, unit="s") as timer:
            result = query()

        if settings.run.log_timings:
            log_query_timing(
                solution=library_name,
                version=library_version or version(library_name),
                query_number=query_number,
                time=timer.took,
            )

        if settings.run.check_results:
            if query_checker is None:
                msg = "cannot check results if no query checking function is provided"
                raise ValueError(msg)
            if settings.scale_factor != 1:
                msg = f"cannot check results when scale factor is not 1, got {settings.scale_factor}"
                raise RuntimeError(msg)
            query_checker(result, query_number)

        if settings.run.show_results:
            print(result)


def check_query_result_pl(result: pl.DataFrame, query_number: int) -> None:
    """Assert that the Polars result of the query is correct."""
    from polars.testing import assert_frame_equal

    expected = _get_query_answer_pl(query_number)
    assert_frame_equal(result, expected, check_dtypes=False)


def check_query_result_pd(result: pd.DataFrame, query_number: int) -> None:
    """Assert that the pandas result of the query is correct."""
    from pandas.testing import assert_frame_equal

    expected = _get_query_answer_pd(query_number)
    # detect which columns are string/extension dtype in the expected answers
    string_cols = [
        col.lower()
        for col in expected.columns
        if pd.api.types.is_string_dtype(expected[col]) or pd.api.types.is_object_dtype(expected[col])
    ]
    # normalize column names to lowercase for comparison
    got = result.reset_index(drop=True).copy()
    got.columns = [c.lower() for c in got.columns]
    exp = expected.copy()
    exp.columns = [c.lower() for c in exp.columns]
    # strip whitespace and unify types for string columns
    for col in string_cols:
        if col in got.columns and col in exp.columns:
            got[col] = got[col].astype(str).str.strip()
            exp[col] = exp[col].astype(str).str.strip()
    # convert any extension arrays (e.g. pyarrow) to numpy arrays for fair comparison
    for col in exp.columns:
        try:
            exp[col] = exp[col].to_numpy()
        except Exception:
            pass

    # normalize any date/datetime-like columns to ISO date strings for comparison
    for col in set(got.columns).intersection(exp.columns):
        try:
            # ignore pandas warning about format inference falling back to dateutil
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    'ignore', message='Could not infer format.*', category=UserWarning
                )
                got[col] = pd.to_datetime(got[col]).dt.strftime('%Y-%m-%d')
                exp[col] = pd.to_datetime(exp[col]).dt.strftime('%Y-%m-%d')
        except Exception:
            pass

    assert_frame_equal(got, exp, check_dtype=False)


def _get_query_answer_pl(query: int) -> pl.DataFrame:
    """Read the true answer to the query from disk as a Polars DataFrame."""
    from polars import read_parquet

    path = settings.paths.answers / f"q{query}.parquet"
    return read_parquet(path)


def _get_query_answer_pd(query: int) -> pd.DataFrame:
    """Read the true answer to the query from disk as a pandas DataFrame."""
    from pandas import read_parquet

    path = settings.paths.answers / f"q{query}.parquet"
    return read_parquet(path, dtype_backend="pyarrow")
