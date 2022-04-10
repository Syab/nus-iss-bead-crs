#!/bin/sh

schema_registry="http://localhost:8081"
response=0

while [ $response -ne 200 ]; do
  response=$(curl --write-out %{http_code} --silent --output /dev/null ${schema_registry})
  sleep 1
done

echo -e "\nSet compatibility for HDB_CPKINFO"
curl -X PUT -H "Content-Type: application/json" \
  --data '{"compatibility": "FULL"}' \
  ${schema_registry}/config/HDB_CPKINFO-value

echo -e "\nSet compatibility for HDB_CPK_AVAILABILITY"
curl -X PUT -H "Content-Type: application/json" \
  --data '{"compatibility": "FULL"}' \
  ${schema_registry}/config/HDB_CPKAVAILABILITY-value