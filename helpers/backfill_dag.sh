#!/bin/bash

# Check if DAG_ID was provided as an argument
if [ -z "$1" ]; then
    echo "Please provide a DAG_ID as the first argument."
    exit 1
fi

# Check if Incremental Window Size was provided as an argument
if [ -z "$2" ]; then
    echo "Please provide a Incremental Window Size as the second argument."
    exit 1
fi

DAG_ID=$1
BLOCK_SIZE=$2 - 1 # Does not include start date so have to minus one
START_DATE="2024-12-01"  # Starting date for backfill
END_DATE=$(date -I -d "$START_DATE + $BLOCK_SIZE days")
TODAY=$(date -I)  # Current date in ISO format

echo "Starting backfill for DAG: $DAG_ID"

# Trigger the backfill process for each 7-day window until we hit today
while [[ "$END_DATE" < "$TODAY" ]]
do
    echo "Triggering backfill from $START_DATE to $END_DATE"
    airflow dags backfill -s $START_DATE -e $END_DATE $DAG_ID > /dev/null

    # Wait for the triggered DAG run to complete
    while true
    do
        # Wait
        echo "Waiting 2 minutes..."
        sleep 120

        # Check if all days have completed successfully
        all_success=true  # Flag to track success for all 7 days
        for DATE in $(seq 0 $BLOCK_SIZE); do
            CHECK_DATE=$(date -I -d "$START_DATE + $DATE days")
            echo "Checking status for $CHECK_DATE..."
            STATUS=$(airflow dags state $DAG_ID $CHECK_DATE)

            echo "STATUS: $STATUS"
            if [[ "$STATUS" == "success" ]]; then
                echo "Run for $CHECK_DATE completed successfully."
            elif [[ "$STATUS" == "running" ]]; then
                echo "Run for $CHECK_DATE still running."
                all_success=false  # If any day is still running, don't move forward
                break
            elif [[ "$STATUS" == "failed" ]]; then
                echo "Run for $CHECK_DATE failed."
                all_success=false  # If any day failed, don't move forward
                break
            fi
        done

        # If all runs are success, exit the loop and move to the next window
        if [ "$ALL_SUCCESS" = true ]; then
            echo "All runs from $START_DATE to $END_DATE completed successfully."
            break
        else
            echo "Not all runs are complete. Checking again in 2 minutes..."
        fi
    done

    # Increment the window to the next period
    START_DATE=$(date -I -d "$END_DATE + 1 day")
    END_DATE=$(date -I -d "$START_DATE + $BLOCK_SIZE days")
done