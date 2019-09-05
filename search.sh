#!/usr/bin/env bash
echo "welcome to Nimbo search engine, Enter your query below:\n"
while :
do
    echo "============================="
    echo "Enter you query please:"
    read  value
    curl -XGET 'localhost:9200/index/type/_search?pretty' -H 'Content-Type: application/json' -d'{"query":{"match":{"content":{"query": "$value","fuzziness": "2"}}}}'
done