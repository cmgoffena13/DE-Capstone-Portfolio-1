CREATE OR REPLACE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 's3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::{AWS_ACCOUNT_ID}:role/snowflake-s3-role'
    STORAGE_ALLOWED_LOCATIONS = ('{s3://polygon-stocks-1/}', 's3://government-trades/');