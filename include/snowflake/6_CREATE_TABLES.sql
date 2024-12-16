CREATE OR REPLACE SCHEMA source

CREATE OR REPLACE TABLE staging.stock_tickers (
    Active BOOLEAN,
    Base_Currency_Name STRING,
    Base_Currency_Symbol STRING,
    Currency_Name STRING,
    Currency_Symbol STRING,
    Last_Updated_UTC TIMESTAMP_TZ,
    Locale STRING,
    Market STRING,
    Name STRING,
    Ticker STRING,
    Row_Hash BINARY,
    watermark_timestamp TIMESTAMP_TZ DEFAULT SYSTIMESTAMP()
);

CREATE OR REPLACE TABLE source.stock_tickers (
    Active BOOLEAN,
    Base_Currency_Name STRING,
    Base_Currency_Symbol STRING,
    Currency_Name STRING,
    Currency_Symbol STRING,
    Last_Updated_UTC TIMESTAMP_TZ,
    Locale STRING,
    Market STRING,
    Name STRING,
    Ticker STRING,
    Row_Hash BINARY,
    watermark_timestamp TIMESTAMP_TZ DEFAULT SYSTIMESTAMP()
);

CREATE OR REPLACE TABLE source.market_close_by_day (
    status STRING,
    date_recorded DATE,
    symbol STRING,
    open DECIMAL(20,4),
    high DECIMAL(20,4),
    low DECIMAL(20,4),
    close DECIMAL(20,4),
    stock_volume BIGINT,
    after_Hours DECIMAL(20,4),
    pre_Market DECIMAL(20,4)
);

CREATE OR REPLACE TABLE source.government_trades (
    amendment_number INT,
    amount STRING,
    chamber STRING,
    disclosure_url STRING,
    
    committees VARIANT,
    display_name STRING,
    district STRING,
    headshot STRING,
    member_record_id STRING,
    leadership_positions VARIANT,
    member_id STRING,--
    member_name STRING,
    party STRING,
    state STRING,
    member_updated BIGINT,--
    website STRING,

    trade_record_id STRING,--
    notification_date DATE,
    ownership STRING,
    report_date DATE,
    report_id BIGINT,
    security_name STRING,
    security_ticker STRING,
    security_type STRING,
    transaction_date DATE,
    transaction_id STRING,
    transaction_type STRING,
    record_updated BIGINT--
);