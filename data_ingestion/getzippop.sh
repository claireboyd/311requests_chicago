#!/bin/bash

# URL of the CSV file
url="https://data.cityofchicago.org/api/views/85cm-7uqa/rows.csv?accessType=DOWNLOAD"

# Download the CSV file using curl
curl -o "ckboyd_zippop.csv" "$url"

# Check if download was successful
if [ $? -eq 0 ]; then
    echo "CSV file downloaded successfully to ckboyd_zippop.csv"
else
    echo "Failed to download CSV file"
fi