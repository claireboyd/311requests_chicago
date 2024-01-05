#!/bin/bash

# URL of the CSV file
url="https://data.cityofchicago.org/api/views/v6vf-nfxy/rows.csv?fourfour=v6vf-nfxy&accessType=DOWNLOAD"

# Download the CSV file using curl
curl -o "ckboyd_311requests.csv" "$url"

# Check if download was successful
if [ $? -eq 0 ]; then
    echo "CSV file downloaded successfully to requests.csv"
else
    echo "Failed to download CSV file"
fi