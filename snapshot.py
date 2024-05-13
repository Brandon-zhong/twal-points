import sys
import polars as pl
from datetime import date, timedelta, datetime, timezone
import numpy as np

sys.path.append("../v3-polars/")
from v3 import state

# configurable 
as_of = 19500000

if __name__ == "__main__":
    # initialize v3-polars
    poolAddress = "0xbf7d01d6cddecb72c2369d1b421967098b10def7"
    update = False

    pool = state.v3Pool(poolAddress, "ethereum", update=update)

    swaps = pool.swaps

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

    tick = pool.getTickAt(as_of)
    lps = (
        mb.filter(
            (pl.col("as_of") <= as_of)
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
    tracked_liquidity = 0
    lp_dict = {}