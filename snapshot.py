import sys
import polars as pl
from datetime import date, timedelta, datetime, timezone
import numpy as np

sys.path.append("../v3-polars/")
from v3 import state

# configurable 
# as_of = 19907818

def queryDataWithBlock(block=19907818, update=False):
    print("queryDataWithBlock", block, update)
    # initialize v3-polars
    poolAddress = "0xBDB04e915B94FbFD6e8552ff7860E59Db7d4499a"

    pool = state.v3Pool(poolAddress, "ethereum", update=update)

    # swaps = pool.swaps

    # key = (from_address-tick_lower-tick_upper)
    # this is how core indexes positions but uses keccack(key) instead
    mb = (pool.mb
          .with_columns(
                    key=(
                        pl.col("from_address")
                        + "-"
                        + pl.col("tick_lower").cast(pl.Utf8)
                        + "-"
                        + pl.col("tick_upper").cast(pl.Utf8)
                    ),
                    liquidity_delta=pl.col("type_of_event") * pl.col("amount"),
                )
        )

    tick = pool.getTickAt(block)
    lps = (
        mb.filter(
            (pl.col("as_of") <= block)
            &
            # positions are in range if tl <= tick < tu
            (pl.col("tick_lower") <= tick)
            & (pl.col("tick_upper") > tick)
        )
        .select(["key", "liquidity_delta"])
        .group_by("key")
        .sum()
        # filter out the empty positions
        .filter(pl.col("liquidity_delta") != 0)
    )

    # TODO
    # this is possible if there was a transfer of liquidity
    # we could instead check via tokenID (which cannot change)
    assert lps.filter(
        pl.col("liquidity_delta") < 0
    ).is_empty(), "Negative liquidity"

    # we know that all lps are positive and in-range
    print(block, update,"================================================")
    # print(lps) 
    # 初始化total
    total_liquidity_delta = 0
    for row in lps.iter_rows(named=True):
        address = row['key'].split('-')[0]
        liquidity_delta = row['liquidity_delta']
        total_liquidity_delta += liquidity_delta
        print(address, liquidity_delta)
    print(total_liquidity_delta)
    return


if __name__ == "__main__":
    # 解析单个参数
    if len(sys.argv) > 1:
        block = sys.argv[1]
        update = sys.argv[2]
        print("Parameter :", block, update)
        queryDataWithBlock(block, update)
    else:
        print("No parameters provided.")
        
    # for block in range (19007818, 19907818):
    #     queryDataWithBlock(block, False)