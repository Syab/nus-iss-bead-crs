#!/bin/sh

schema_registry="http://localhost:8081"
response=0

echo -e "\n Waiting for schema"

deploy_schemas() {
  cd ./schemas

  fileList=$(ls *.avsc)
  for file in ${fileList}; do
    fileContent=$(cat $file)

    s=$(echo $fileContent | jq '.aliases[0]')
    echo ${s}
    s1=${s%\"}
    echo ${s1}
    topicName=${s1#\"}
    echo ${topicName}

    url="${schema_registry}/subjects/${topicName}-value/versions"
    echo ${url}
    echo $(echo $fileContent | jq '{"schema":.|tostring}') >dump.txt

    echo "Processing: $file\nUrl: $url"
    curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" -d "@dump.txt" ${url}
    echo "\n"

    rm dump.txt
  done

  sh ../schema-compatibility.sh

}

while [ $response -ne 200 ]; do
  response=$(curl --write-out %{http_code} --silent --output /dev/null ${schema_registry})
  if [ $response -eq 200 ]; then
    echo "schema OK. Time to deploy"
    deploy_schemas
  fi
  sleep 1
done