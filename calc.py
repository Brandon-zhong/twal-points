import sys
import polars as pl
from datetime import date, timedelta, datetime, timezone
import numpy as np

sys.path.append("../v3-polars/")
from v3 import state

"""
TODO

- I am checking liquidity by a manufactured key (which may get transferred)
I need to calculate the key if not nft position manager
This can be solved by using tokenid

- We distribute as of last swap in the block
However, this may pick up JIT liquidity. We can calculate as_of the end of the block,
but this will mean we cannot check that we are arriving at the correct number
However, I am confident that we are

- I am only distributing rewards for the creator of a position
We could add a check for transfers to the mb frame
"""

# configurable 
t_0 = datetime(year = 2024, month = 4, day = 1)
t_1 = datetime(year = 2024, month = 4, day = 2)

if __name__ == "__main__":
    # initialize v3-polars
    poolAddress = "0xbf7d01d6cddecb72c2369d1b421967098b10def7"
    update = False

    pool = state.v3Pool(poolAddress, "ethereum", update=update)

    swaps = pool.swaps
    mb = pool.mb

    # key = (from_address-tick_lower-tick_upper)
    # this is how core indexes positions but uses keccack(key) instead
    mb = mb.with_columns(
        key=(
            pl.col("from_address")
            + "-"
            + pl.col("tick_lower").cast(pl.Utf8)
            + "-"
            + pl.col("tick_upper").cast(pl.Utf8)
        ),
        liquidity_delta=pl.col("type_of_event") * pl.col("amount"),
    )

    tgt_swaps = (
        # we want only the last tx to ensure we arent checking for mid swap stuff
        # and to ensure the time weight isnt just 0
        swaps.join(
            # find the last transaction in the block
            (
                swaps.select(["block_number", "transaction_index"])
                .group_by("block_number")
                .last()
                .rename({"transaction_index": "last_index"})
            ),
            on="block_number",
            how="inner",
        )
        # drop all txs that arent the final swap
        .filter(pl.col("last_index") == pl.col("transaction_index"))
        .sort("block_number")
        # calculate time-weight
        .with_columns(twal=pl.col("block_timestamp").diff().dt.seconds())
        .filter(~pl.col("twal").is_null())
        .filter(
                (pl.col('block_timestamp') <= t_1.replace(tzinfo = timezone.utc))
        )
    )

    subset = tgt_swaps.filter(pl.col('block_timestamp') >= t_0.replace(tzinfo = timezone.utc))
    
    if subset.is_empty():
        subset = tgt_swaps.tail(1)

    # main take swaps and mints/burns -> share at each swap block
    data = []
    iterator = subset.select(
        ["block_number", "transaction_index", "tick", "liquidity", "twal"]
    ).sort("block_number")

    for bn, tx_index, tick, liquidity, twal in iterator.iter_rows():
        tick, liquidity = int(tick), int(liquidity)

        # decimalized version of block, index
        as_of = bn + tx_index / 1e4

        # cxalculate all lps in range as of the period we want
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

        # calculate their shares
        for key, liquidity_delta in lps.iter_rows():
            tracked_liquidity += liquidity_delta

            lp_dict[key] = liquidity_delta

        # we know that all liquidity is tracked and their shares
        # most likely issue here is a floating point error
        assert np.isclose(
            float(liquidity), float(tracked_liquidity)
        ), "Missing liquidity"

        data.append([bn, twal, liquidity, lp_dict.copy()])

    # calculate share of each lp at each time
    shares = {}
    total_time = 0
    for bn, twal, liquidity, lps in data:
        total_time += twal
        for lp in lps.keys():
            # schema = (address, tl, tu)
            address = lp.split("-")[0]

            current_share = shares.get(address, 0)
            shares[address] = current_share + twal * (lps[lp] / liquidity)

    assert np.isclose(total_time, sum(shares.values())), "Missing share of time"

    for lp in shares.keys():
        shares[lp] = shares[lp] / total_time

    # we did it joe
    print(shares)
