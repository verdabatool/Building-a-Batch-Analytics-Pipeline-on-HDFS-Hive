#!/bin/bash

# Adjust this path if your Hadoop bin directory is located elsewhere:
HDFS_CMD="/home/hadoope/Hadoop/bin/hdfs dfs"

# Check if a date parameter is provided
if [ -z "$1" ]; then
    echo "Usage: $0 YYYY-MM-DD"
    exit 1
fi

# Assign the input date to a variable
DATE=$1

# Parse year, month, and day from the date parameter
year=$(echo $DATE | cut -d'-' -f1)
month=$(echo $DATE | cut -d'-' -f2)
day=$(echo $DATE | cut -d'-' -f3)

echo "Ingesting data for date: $DATE"
echo "Year: $year, Month: $month, Day: $day"

# Define HDFS directories for logs and metadata
LOG_HDFS_DIR="/raw/logs/$year/$month/$day"
META_HDFS_DIR="/raw/metadata/$year/$month/$day"

# Create the HDFS directories (if they don't already exist)
$HDFS_CMD -mkdir -p $LOG_HDFS_DIR
$HDFS_CMD -mkdir -p $META_HDFS_DIR

# Define local file names (assumes files are in the current directory)
LOCAL_LOG_FILE="${DATE}.csv"
LOCAL_META_FILE="content_metadata.csv"

# Ingest the daily log file into HDFS
if [ -f "$LOCAL_LOG_FILE" ]; then
    echo "Copying log file: $LOCAL_LOG_FILE to HDFS directory: $LOG_HDFS_DIR"
    $HDFS_CMD -put -f $LOCAL_LOG_FILE $LOG_HDFS_DIR
else
    echo "Log file $LOCAL_LOG_FILE not found in the current directory."
fi

# Ingest the content metadata file into HDFS
if [ -f "$LOCAL_META_FILE" ]; then
    echo "Copying metadata file: $LOCAL_META_FILE to HDFS directory: $META_HDFS_DIR"
    $HDFS_CMD -put -f $LOCAL_META_FILE $META_HDFS_DIR
else
    echo "Metadata file $LOCAL_META_FILE not found in the current directory."
fi

echo "Ingestion completed for date: $DATE"



