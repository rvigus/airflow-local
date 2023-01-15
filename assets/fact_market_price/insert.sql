insert into mart.fact_market_price(dim_date_id, dim_ticker_id, open, high, low, close, volume, row_created_ts, row_last_updated_ts)
select
    dd.dim_date_id,
    dtk.dim_ticker_id,
    cast(src.open as double precision) as open,
    cast(src.high as double precision) as high,
    cast(src.low as double precision) as low,
    cast(src.close as double precision) as close,
    cast(src.volume as int) as volume,
    now() as row_created_ts,
    now() as row_last_updated_ts
from src.market_prices src
    join mart.dim_date dd on dd.dim_date = src.index::date
    join mart.dim_ticker dtk on dtk.ticker = src.symbol
order by dd.dim_date_id, dtk.dim_ticker_id, src.index asc;