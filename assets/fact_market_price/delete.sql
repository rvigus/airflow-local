delete from mart.fact_market_price fmp
where exists (
    select * from mart.fact_market_price
        join mart.dim_date dd on dd.dim_date_id = fmp.dim_date_id
        join mart.dim_ticker dtk on dtk.dim_ticker_id = fmp.dim_ticker_id
        join src.market_prices src on src.index::date = dd.dim_date and src.symbol = dtk.ticker
);