#!/bin/bash

# Define variables
totalTime=0
count=10
queryFile="$1"
# Loop to execute the query file multiple times
for i in $(seq 1 $count); do
    # Execute the query, extract the time in milliseconds, remove commas
    brew services restart postgresql@16
    sleep 1.5
    time=$(psql -U mosix11 -h localhost -d soccer -c "\timing on" -f $queryFile 2>&1 | grep "Time:" | awk '{print $2}' | tr -d ',')
    
    # Check if time is a number, then add to total
    if [[ $time =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        totalTime=$(echo "$totalTime + $time / 1000" | bc)
    else
        echo "Non-numeric time encountered: $time"
    fi
done

# Calculate the average time
if [ $count -gt 0 ]; then
    averageTime=$(echo "scale=2; $totalTime / $count" | bc)
    echo "Average execution time: $averageTime ms"
else
    echo "Count must be greater than zero."
fi
