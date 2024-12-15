CREATE OR REPLACE STAGE STOCK_DB.external_stages.stock_tickers
    storage_integration=s3_integration
    url='s3://polygon-stocks-1/stock-tickers/';

CREATE OR REPLACE STAGE STOCK_DB.external_stages.market_close
    storage_integration=s3_integration
    url='s3://polygon-stocks-1/market-close/';

CREATE OR REPLACE STAGE STOCK_DB.external_stages.government_trades
    storage_integration=s3_integration
    url='s3://government-trades';