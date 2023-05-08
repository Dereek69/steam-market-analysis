WITH
  --get all events on the jay contract
  jay_events AS (
    SELECT
      topic0,
      topic1,
      topic2,
      topic3,
      tx_hash as hash,
      block_time as start_time,
      data as data
    from
      ethereum.logs as logs
    where
      contract_address = 0xDA7C0810cE6F8329786160bb3d1734cf6661CA6E
  ),
  --get the hashes of all events on the jay contract
  jay_hashes AS (
    SELECT DISTINCT
      hash
    from
      jay_events
  ),
  --get the transactions that correspond to the hash of the jay events
  jay_transactions AS (
    SELECT
      eth.hash,
      eth.data,
      eth."from" as sender,
      eth.block_time as time
    FROM
      ethereum.transactions as eth
      RIGHT JOIN jay_hashes as jay ON jay.hash = eth.hash
  ),
  --filter all the transactions that call the buy function
  buy_txs AS (
    SELECT
      *
    FROM
      jay_transactions
    WHERE
      bytearray_substring (data, 1, 4) = 0xf088d547
  ),
  --filter all the transactions that call the sell function
  sell_txs AS (
    SELECT
      *
    FROM
      jay_transactions
    WHERE
      bytearray_substring (data, 1, 4) = 0xe4849b32
  ),
  --cross references the buy transactions and the buy events to get the data from the first event of the buy txs
  buys_1 AS (
    SELECT
      jay.hash,
      bytearray_to_uint256 (bytearray_substring (jay.data, 1, 32)) as time,
      bytearray_to_uint256 (bytearray_substring (jay.data, 33, 32)) as received,
      bytearray_to_uint256 (bytearray_substring (jay.data, 65, 32)) as sent
    FROM
      jay_events as jay
      INNER JOIN buy_txs ON jay.hash = buy_txs.hash
    WHERE
      topic0 = 0x4afcb4a87cdbd9974efdb92ee48bc8d7cd0ae4bf217004db3d080cbaee652ca7
  ),
  --same thing for the second event
  buys_2 AS (
    SELECT
      jay.hash,
      bytearray_substring (jay.topic2, 13, 20) as minter,
      bytearray_to_uint256 (bytearray_substring (jay.data, 1, 32)) as minted
    FROM
      jay_events as jay
      INNER JOIN buy_txs ON jay.hash = buy_txs.hash
    WHERE
      topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
  ),
  --combine the two buy events into one table
  buys AS (
    SELECT
      n1.hash,
      n2.minter as minter,
      n2.minted as jay,
      from_unixtime(CAST(n1.time AS bigint)) as time,
      n1.sent as eth_spent,
      (n1.sent / 33) / 3 as team_fee,
      (n1.sent / 33) / 3 as staking_fee,
      (n1.sent / 33) / 3 as nft_fee,
      (n1.sent * 0.1) - (n1.sent / 33) as fee
    FROM
      buys_1 as n1
      FULL OUTER JOIN buys_2 as n2 ON n1.hash = n2.hash
  ),
  --repeats the previous events for the sell transactions and events
  sells_1 AS (
    SELECT
      jay.hash,
      bytearray_to_uint256 (bytearray_substring (jay.data, 1, 32)) as time,
      bytearray_to_uint256 (bytearray_substring (jay.data, 33, 32)) as received,
      bytearray_to_uint256 (bytearray_substring (jay.data, 65, 32)) as sent
    FROM
      jay_events as jay
      INNER JOIN sell_txs ON jay.hash = sell_txs.hash
    WHERE
      topic0 = 0x4afcb4a87cdbd9974efdb92ee48bc8d7cd0ae4bf217004db3d080cbaee652ca7
  ),
  sells_2 AS (
    SELECT
      jay.hash,
      bytearray_substring (jay.topic2, 13, 20) as minter,
      bytearray_to_uint256 (bytearray_substring (jay.data, 1, 32)) as minted
    FROM
      jay_events as jay
      INNER JOIN sell_txs ON jay.hash = sell_txs.hash
    WHERE
      topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
  ),
  sells AS (
    SELECT
      n1.hash,
      n2.minter as minter,
      n2.minted as jay,
      from_unixtime(CAST(n1.time AS bigint)) as time,
      (n1.sent * 0.9) as eth_received,
      (n1.sent / 33) / 3 as team_fee,
      (n1.sent / 33) / 3 as staking_fee,
      (n1.sent / 33) / 3 as nft_fee,
      (n1.sent * 0.1) - (n1.sent / 33) as fee
    FROM
      sells_1 as n1
      FULL OUTER JOIN sells_2 as n2 ON n1.hash = n2.hash
  ),
  --reduces all the data to an hourly table and joins the buys and sells tables.
  hourly_data AS (
    SELECT
      date_trunc('hour', coalesce(sells.time, buys.time)) as time,
      sum(coalesce(CAST(buys.jay AS DECIMAL (38, 0)), 0)) / power(10, 18) as minted,
      sum(coalesce(CAST(sells.jay AS DECIMAL (38, 0)), 0)) / power(10, 18) as burned,
      sum(
        coalesce(CAST(buys.eth_spent AS DECIMAL (38, 0)), 0)
      ) / power(10, 18) as eth_spent,
      sum(
        coalesce(CAST(sells.eth_received AS DECIMAL (38, 0)), 0)
      ) / power(10, 18) as eth_received,
      sum(
        (
          coalesce(CAST(buys.team_fee AS DECIMAL (38, 0)), 0) * 3
        ) + (
          coalesce(CAST(sells.team_fee AS DECIMAL (38, 0)), 0) * 3
        )
      ) / power(10, 18) as other_fees
    FROM
      sells
      FULL OUTER JOIN buys on sells.time = buys.time
    GROUP BY
      1
  ),
  --cumulative data to get the total supply and fees accrued
  cumulative AS (
    SELECT
      time,
      sum(minted) OVER (
        ORDER BY
          time
      ) as cum_minted,
      sum(burned) OVER (
        ORDER BY
          time
      ) as cum_burned,
      sum(eth_spent) OVER (
        ORDER BY
          time
      ) as cum_eth_spent,
      sum(eth_received) OVER (
        ORDER BY
          time
      ) as cum_eth_received,
      sum(other_fees) OVER (
        ORDER BY
          time
      ) as cum_other_fees
    FROM
      hourly_data
  ),
  --get the intrinsic value of the token given the current outstanding supply, the supplied and withdrawn eth, and the fees accrued
  intrinsic_value AS (
    SELECT DISTINCT
      date_trunc('hour', time) as time,
      (cum_minted - cum_burned) / power(10, 18) as supply,
      (cum_eth_spent - cum_eth_received - cum_other_fees) / (cum_minted - cum_burned) as intrinsic_price,
      (
        (cum_eth_spent - cum_eth_received - cum_other_fees) / (cum_minted - cum_burned)
      ) / 0.9 as eth_buy_price,
      (
        (cum_eth_spent - cum_eth_received - cum_other_fees) / (cum_minted - cum_burned)
      ) * 0.9 as eth_sell_price
    FROM
      cumulative
  ORDER BY time ASC
  ),
  --calculate the hourly fees accrued
  fees_accrued AS (
    SELECT DISTINCT
      coalesce(sells.hour, buys.hour) as time,
      sell_fees,
      sell_team_fees,
      sell_staking_fees,
      sell_nft_fees,
      buy_fees,
      buy_team_fees,
      buy_staking_fees,
      buy_nft_fees
    FROM
      (
        SELECT
          date_trunc('hour', time) as hour,
          sum(coalesce(CAST(sells.fee AS DECIMAL (38, 0)), 0)) / power(10, 18) as sell_fees,
          sum(
            coalesce(CAST(sells.team_fee AS DECIMAL (38, 0)), 0)
          ) / power(10, 18) as sell_team_fees,
          sum(
            coalesce(CAST(sells.staking_fee AS DECIMAL (38, 0)), 0)
          ) / power(10, 18) as sell_staking_fees,
          sum(
            coalesce(CAST(sells.nft_fee AS DECIMAL (38, 0)), 0)
          ) / power(10, 18) as sell_nft_fees
        FROM
          sells
        GROUP BY
          1
      ) AS sells
      FULL OUTER JOIN (
        SELECT
          date_trunc('hour', time) as hour,
          sum(coalesce(CAST(buys.fee AS DECIMAL (38, 0)), 0)) / power(10, 18) as buy_fees,
          sum(
            coalesce(CAST(buys.team_fee AS DECIMAL (38, 0)), 0)
          ) / power(10, 18) as buy_team_fees,
          sum(
            coalesce(CAST(buys.staking_fee AS DECIMAL (38, 0)), 0)
          ) / power(10, 18) as buy_staking_fees,
          sum(
            coalesce(CAST(buys.nft_fee AS DECIMAL (38, 0)), 0)
          ) / power(10, 18) as buy_nft_fees
        FROM
          buys
        GROUP BY
          1
      ) AS buys ON sells.hour = buys.hour
  ORDER BY time ASC
  ),
  --get the houlry price of eth in usd
  eth_price AS (
    SELECT DISTINCT
      date_trunc('hour', minute) as time,
      avg(price) as price
    FROM
      prices.usd
    where
      contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
      AND blockchain = 'ethereum'
    GROUP BY date_trunc('hour', minute)
  ORDER BY time ASC
  ),
  --checks the pool trades for the jay/usdc pool
  jay_pool_buys AS (
    SELECT
      block_time as time,
      token_sold_amount / token_bought_amount as jay_price
    FROM
      uniswap_v2_ethereum.trades
    WHERE
      project_contract_address = 0xEb7b5294c79b0369315Ed7fE76b52d3108B0A62D
      AND token_bought_symbol = 'JAY'
  ),
  jay_pool_sells AS (
    SELECT
      block_time as time,
      token_bought_amount / token_sold_amount as jay_price
    FROM
      uniswap_v2_ethereum.trades
    WHERE
      project_contract_address = 0xEb7b5294c79b0369315Ed7fE76b52d3108B0A62D
      AND token_sold_symbol = 'JAY'
  ),
  --combine the buy and sell prices to make a continuous price feed
  jay_usdc_price AS (
    SELECT DISTINCT
      date_trunc('hour', coalesce(buys.time, sells.time)) as time,
      coalesce(avg(buys.jay_price), avg(sells.jay_price)) as jay_price
    FROM
      jay_pool_buys as buys
      FULL OUTER JOIN jay_pool_sells as sells ON buys.time = sells.time
    GROUP BY date_trunc('hour', coalesce(buys.time, sells.time))
  ORDER BY time ASC
  ),
  --combine all the data into one table and fills the empty holes
  complete AS (
    SELECT DISTINCT
      coalesce(
        intrinsic_value.time,
        jay_usdc_price.time,
        fees_accrued.time,
        eth_price.time
      ) as time,
      coalesce(intrinsic_value.supply, LAG(intrinsic_value.supply) OVER (ORDER BY intrinsic_value.time)) as total_supply,
      coalesce(intrinsic_value.intrinsic_price, LAG(intrinsic_value.intrinsic_price) OVER (ORDER BY intrinsic_value.time)) as eth_intrinsic_price,
      coalesce(intrinsic_value.eth_buy_price, LAG(intrinsic_value.eth_buy_price) OVER (ORDER BY intrinsic_value.time)) as website_buy_price_eth,
      coalesce(intrinsic_value.eth_sell_price, LAG(intrinsic_value.eth_sell_price) OVER (ORDER BY intrinsic_value.time)) as website_sell_price_eth,
      coalesce(eth_price.price, LAG(eth_price.price) OVER (ORDER BY eth_price.time)) as ethereum_price,
      coalesce(jay_usdc_price.jay_price, LAG(jay_usdc_price.jay_price) OVER (ORDER BY jay_usdc_price.time)) as pool_price_usdc,
      coalesce(fees_accrued.sell_fees, 0) as sell_fees,
      coalesce(fees_accrued.sell_team_fees, 0) as sell_team_fees,
      coalesce(fees_accrued.sell_staking_fees, 0) as sell_staking_fees,
      coalesce(fees_accrued.sell_nft_fees, 0) as sell_nft_fees,
      coalesce(fees_accrued.buy_fees, 0) as buy_fees,
      coalesce(fees_accrued.buy_team_fees, 0) as buy_team_fees,
      coalesce(fees_accrued.buy_staking_fees, 0) as buy_staking_fees,
      coalesce(fees_accrued.buy_nft_fees, 0) as buy_nft_fees
    FROM
      intrinsic_value
      FULL OUTER JOIN jay_usdc_price ON intrinsic_value.time = jay_usdc_price.time
      LEFT JOIN eth_price ON eth_price.time = intrinsic_value.time
      FULL OUTER JOIN fees_accrued ON fees_accrued.time = intrinsic_value.time
  )
  
  SELECT
  *,
  eth_intrinsic_price * ethereum_price as usdc_intrinsic_price,
  website_sell_price_eth * ethereum_price as website_sell_price_usdc,
  website_buy_price_eth * ethereum_price as website_buy_price_usdc
  
  FROM
  complete
  ORDER BY time ASC