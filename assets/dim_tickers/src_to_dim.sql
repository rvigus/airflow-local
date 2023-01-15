
-- update existing records with latest info
update mart.dim_ticker
set company = src.company,
  sector = src.sectors,
  market_index = src.indice
from src.ftse_tickers src
where ticker = src.tickers;

-- insert new tickers that don't exist.
insert into mart.dim_ticker (ticker, company, sector, market_index)
select src.tickers, src.company, src.sectors, src.indice
from src.ftse_tickers src
    left join mart.dim_ticker dest on dest.ticker = src.tickers
where dest.ticker is NULL;