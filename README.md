# twal-points

### What we calculate
At each swap, we calculate the total amount of liquidity provided by LPs and split the share of time by their share of liquidty from the current swap to the previous swap.

This incentivizes long-term liquidity provision as liquidity providers are only incentivized when their liquidity is used.

### How to use
Requires [v3-polars](https://github.com/Uniswap/v3-polars) and an API key.
Provide a ``t_0`` and a ``t_1`` to calculate time-weighted average liquidity provided by each liquidity provider address

### Example
Calcualates liquidity provided by each LP on the [pufETH/WETH](https://app.uniswap.org/explore/pools/ethereum/0xBF7d01D6cDdECb72c2369d1B421967098B10deF7) 5 bps pool on Uniswap v3

