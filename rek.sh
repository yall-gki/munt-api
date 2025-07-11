#!/bin/bash

URL="http://localhost:8000/prices"

while true; do
  curl -s $URL -o /dev/null -w "HTTP status: %{http_code}\n"
  sleep 0.1  # 100 milliseconds
done
