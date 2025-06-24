from queries.exasol import utils

Q_NUM = 6


def q() -> None:
    query_str = utils.get_sql_query(Q_NUM)
    utils.run_query(Q_NUM, query_str)


if __name__ == "__main__":
    q()

