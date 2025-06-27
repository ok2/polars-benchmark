# Polars Decision Support (PDS) benchmarks

## Disclaimer

Polars Decision Support (PDS) benchmarks are derived from the TPC-H Benchmarks and as such any results obtained using PDS are not comparable to published TPC-H Benchmark results, as the results obtained from using PDS do not comply with the TPC-H Benchmarks.

These benchmarks are our adaptation of an industry-standard decision support benchmark often used in the DataFrame library community. PDS consists of the same 22 queries as the industry standard benchmark TPC-H, but has modified parts for dataset generation and execution scripts.

From the [TPC website](https://www.tpc.org/tpch/):
> TPC-H is a decision support benchmark. It consists of a suite of business-oriented ad hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance. This benchmark illustrates decision support systems that examine large volumes of data, execute queries with a high degree of complexity, and give answers to critical business questions.

## License

PDS is licensed under Apache License, Version 2.0.

Additionally, certain files in PDS are licensed subject to the accompanying [TPC EULA](TPC%20EULA.txt) (also available at <http://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp>). Files subject to the TPC EULA are identified as such within the files.

You may not use PDS except in compliance with the Apache License, Version 2.0 and the TPC EULA.

## Generating PDS Benchmarking Data

### Project setup

```shell
# clone this repository
git clone https://github.com/pola-rs/polars-benchmark.git
cd polars-benchmark/tpch-dbgen

# build tpch-dbgen
make
```

### Execute

```shell
# change directory to the root of the repository
cd ../
./run.sh
```

This will do the following,

- Create a new virtual environment with all required dependencies.
- Generate data for benchmarks.
- Load the generated data into Exasol using `IMPORT FROM LOCAL`.
- Run the benchmark suite.

### Loading data into Exasol

If you only want to import the generated tables into an Exasol database, run:

```shell
make load-exasol
```

The command will generate the raw TPC-H `.tbl` files if needed, connect using
the credentials defined in `.env` or environment variables, create the TPC-H
tables if needed, and issue `IMPORT FROM LOCAL` statements for each table.

### Running benchmarks

Once data is prepared (and optionally loaded into Exasol), you can run specific benchmarks via `make`:

| Make target        | Description                                   |
|--------------------|-----------------------------------------------|
| `make run-polars`  | Run Polars benchmarks                         |
| `make run-duckdb`  | Run DuckDB benchmarks                         |
| `make run-exasol`  | Run Exasol benchmarks                         |
| `make run-pandas`  | Run pandas benchmarks                         |
| `make run-pyspark` | Run PySpark benchmarks                        |
| `make run-dask`    | Run Dask benchmarks                           |
| `make run-modin`   | Run Modin benchmarks                          |
| `make run-all`     | Run all benchmarks (including Exasol)         |
| `make plot`        | Generate plots from benchmark results         |
| `make clean`       | Remove generated data and cleanup environment |

You can also run all benchmarks and generate plots in one step:

```shell
# Run benchmarks without IO overhead (cached IO)
make run-all plot

# Run benchmarks including IO overhead
RUN_INCLUDE_IO=1 make run-all plot
```

Use the `SCALE_FACTOR` environment variable to adjust the dataset size
(e.g., `SCALE_FACTOR=10` for larger data).
