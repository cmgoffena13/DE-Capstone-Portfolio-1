#!/bin/bash

# Check if DAG_ID was provided as an argument
if [ -z "$1" ]; then
    echo "Please provide a DAG_ID as the first argument."
    exit 1
fi
# Check if START_DATE was provided as an argument
if [ -z "$2" ]; then
    echo "Please provide a START_DATE as the first argument."
    exit 1
fi
# Check if Incremental Window Size was provided as an argument
if [ -z "$3" ]; then
    echo "Please provide a WINDOW_SIZE as the second argument."
    exit 1
fi

DAG_ID=$1
START_DATE=$2  # Starting date for backfill
WINDOW_SIZE=$(( $3 - 1 )) # Does not include start date so have to minus one
END_DATE=$(date -I -d "$START_DATE + $WINDOW_SIZE days")
TODAY=$(date -I)  # Current date in ISO format

echo "Starting backfill for DAG: $DAG_ID"

# Trigger the backfill process for each 7-day window until we hit today
while [[ "$END_DATE" < "$TODAY" ]]
do
    echo "Running window from $START_DATE to $END_DATE"

    for DATE in $(seq 0 $WINDOW_SIZE); do
        CHECK_DATE=$(date -I -d "$START_DATE + $DATE days")
        echo -e "\tTriggering dag run for $CHECK_DATE"
        airflow dags trigger -e $CHECK_DATE $DAG_ID > /dev/null
    done

    # Wait for the triggered DAG run to complete
    while true
    do
        echo -e "\tWaiting 2 minutes..."
        sleep 120

        # Check if all days have completed successfully
        ALL_SUCCESS=true  # Flag to track success for all 7 days
        FAILURE=false
        for DATE in $(seq 0 $WINDOW_SIZE); do
            CHECK_DATE=$(date -I -d "$START_DATE + $DATE days")
            echo -e "\tChecking status for $CHECK_DATE..."
            STATUS=$(airflow dags state $DAG_ID $CHECK_DATE | tail -n 1)

            if [[ "$STATUS" == "success" ]]; then
                echo -e "\tRun for $CHECK_DATE completed successfully."
            elif [[ "$STATUS" == "running" ]]; then
                echo -e "\tRun for $CHECK_DATE still running."
                ALL_SUCCESS=false  # If any day is still running, don't move forward
                break
            elif [[ "$STATUS" == "failed" ]]; then
                echo -e "\tRun for $CHECK_DATE failed, stopping."
                FAILURE=true
                ALL_SUCCESS=false  # If any day failed, don't move forward
                break
            else
                echo -e "\tSTATUS: $STATUS \n\tNot success, running, or failed; stopping."
                FAILURE=true
                ALL_SUCCESS=false
            fi
        done

        # If all runs are success, exit the loop and move to the next window
        if [ "$ALL_SUCCESS" = true ]; then
            echo -e "\tAll runs from $START_DATE to $END_DATE completed successfully.\n"
            break
        elif [ "$FAILURE" = true ]; then
            echo "Experienced failure in a dag run, stopping."
            exit 1
        else
            echo -e "\tNot all window runs are complete."
        fi
    done

    # Increment the window to the next period
    START_DATE=$(date -I -d "$END_DATE + 1 day")
    END_DATE=$(date -I -d "$START_DATE + $WINDOW_SIZE days")
done
echo "All runs complete."