from queries.exasol import utils

Q_NUM = 20


def q() -> None:

    line_item_ds = utils.get_line_item_ds()
    nation_ds = utils.get_nation_ds()
    part_ds = utils.get_part_ds()
    part_supp_ds = utils.get_part_supp_ds()
    supplier_ds = utils.get_supplier_ds()
    query_str = f"""
    select
            s_name,
            s_address
        from
            {supplier_ds},
            {nation_ds}
        where
            s_suppkey in (
                select
                    ps_suppkey
                from
                    {part_supp_ds}
                where
                    ps_partkey in (
                        select
                            p_partkey
                        from
                            {part_ds}
                        where
                            p_name like 'forest%'
                    )
                    and ps_availqty > (
                        select
                            0.5 * sum(l_quantity)
                        from
                            {line_item_ds}
                        where
                            l_partkey = ps_partkey
                            and l_suppkey = ps_suppkey
                            and l_shipdate >= date '1994-01-01'
                            and l_shipdate < date '1994-01-01' + interval '1' year
                    )
            )
            and s_nationkey = n_nationkey
            and n_name = 'CANADA'
        order by
            s_name
        ;
    """
    utils.run_query(Q_NUM, query_str)


if __name__ == "__main__":
    q()

