CREATE OR REPLACE PROCEDURE staging.merge_stock_tickers()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
try {
  // MERGE statement with deduplicated staging data
  var merge_sql = `
    MERGE INTO source.stock_tickers AS target
    USING (SELECT *
      FROM (
        SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Last_Updated_UTC DESC) AS row_num
        FROM staging.stock_tickers
      )
      WHERE row_num = 1
    ) AS source
    ON target.Ticker = source.Ticker
    WHEN MATCHED AND (target.Row_Hash != source.Row_Hash)
      THEN
        UPDATE SET
          target.Active = source.Active,
          target.Base_Currency_Name = source.Base_Currency_Name,
          target.Base_Currency_Symbol = source.Base_Currency_Symbol,
          target.Currency_Name = source.Currency_Name,
          target.Currency_Symbol = source.Currency_Symbol,
          target.Last_Updated_UTC = source.Last_Updated_UTC,
          target.Locale = source.Locale,
          target.Market = source.Market,
          target.Name = source.Name,
          target.Ticker = source.Ticker,
          target.Row_Hash = source.Row_Hash,
          target.watermark_timestamp = source.watermark_timestamp
    WHEN NOT MATCHED
      THEN
        INSERT (Active, Base_Currency_Name, Base_Currency_Symbol, Currency_Name, Currency_Symbol, 
                Last_Updated_UTC, Locale, Market, Name, Ticker, Row_Hash, watermark_timestamp)
        VALUES (source.Active, source.Base_Currency_Name, source.Base_Currency_Symbol, source.Currency_Name, 
                source.Currency_Symbol, source.Last_Updated_UTC, source.Locale, source.Market, source.Name, 
                source.Ticker, source.Row_Hash, source.watermark_timestamp);`;

  // Execute the MERGE statement
  var merge_statement = snowflake.createStatement({sqlText: merge_sql});
  merge_statement.execute();

  // DELETE statement
  var delete_sql = `
  DELETE FROM source.stock_tickers AS target
  WHERE NOT EXISTS (
      SELECT 1
      FROM staging.stock_tickers AS source
      WHERE target.Ticker = source.Ticker
  );`;

  // Execute the DELETE statement
  var delete_statement = snowflake.createStatement({sqlText: delete_sql});
  delete_statement.execute();

  return "Merge and delete operations completed successfully!";
} catch(err) {
  return "Failed: " + err; 
}
$$;