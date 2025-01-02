CREATE OR REPLACE PROCEDURE handle_stock_market_duplicates()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
AS
$$
try {
    // Start a transaction
    var sql_command = "BEGIN TRANSACTION;";
    var statement1 = snowflake.createStatement({sqlText: sql_command});
    statement1.execute();

    // Create the duplicate_holder table
    sql_command = `
        CREATE OR REPLACE TRANSIENT TABLE duplicate_holder AS (
            SELECT
                Status,
                Date_Recorded,
                Symbol,
                Open,
                High,
                Low,
                Close,
                Stock_Volume,
                After_Hours,
                Pre_Market
            FROM source.market_close_by_day
            WHERE Date_Recorded >= CURRENT_DATE - INTERVAL '70 DAY'
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
            HAVING COUNT(*) > 1
        );
    `;
    var statement2 = snowflake.createStatement({sqlText: sql_command});
    statement2.execute();

    // Delete the duplicate rows
    sql_command = `
        DELETE FROM source.market_close_by_day AS a
        USING duplicate_holder AS b
        WHERE 
            IFNULL(CAST(a.Status AS STRING), '') = IFNULL(CAST(b.Status AS STRING), '') AND
            IFNULL(CAST(a.Date_Recorded AS STRING), '') = IFNULL(CAST(b.Date_Recorded AS STRING), '') AND
            IFNULL(CAST(a.Symbol AS STRING), '') = IFNULL(CAST(b.Symbol AS STRING), '') AND
            IFNULL(CAST(a.Open AS STRING), '') = IFNULL(CAST(b.Open AS STRING), '') AND
            IFNULL(CAST(a.High AS STRING), '') = IFNULL(CAST(b.High AS STRING), '') AND
            IFNULL(CAST(a.Low AS STRING), '') = IFNULL(CAST(b.Low AS STRING), '') AND
            IFNULL(CAST(a.Close AS STRING), '') = IFNULL(CAST(b.Close AS STRING), '') AND
            IFNULL(CAST(a.Stock_Volume AS STRING), '') = IFNULL(CAST(b.Stock_Volume AS STRING), '') AND
            IFNULL(CAST(a.After_Hours AS STRING), '') = IFNULL(CAST(b.After_Hours AS STRING), '') AND
            IFNULL(CAST(a.Pre_Market AS STRING), '') = IFNULL(CAST(b.Pre_Market AS STRING), '');
    `;
    var statement3 = snowflake.createStatement({sqlText: sql_command});
    statement3.execute();

    // Insert the data back
    sql_command = "INSERT INTO source.market_close_by_day SELECT * FROM duplicate_holder;";
    var statement4 = snowflake.createStatement({sqlText: sql_command});
    statement4.execute();

    // Commit the transaction
    sql_command = "COMMIT;";
    var statement5 = snowflake.createStatement({sqlText: sql_command});
    statement5.execute();

    return "Transaction completed successfully.";
} catch (err) {
    // Rollback the transaction in case of an error
    var sql_command = "ROLLBACK;";
    var statement6 = snowflake.createStatement({sqlText: sql_command});
    statement6.execute();

    return "Error: " + err.message;
}
$$;