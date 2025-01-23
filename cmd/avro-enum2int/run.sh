#!/bin/sh

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
	cat ./sample.d/sample.jsonl |
		json2avrows |
		cat > ./sample.d/sample.avro
}

#genavro

export ENV_ENUM_SCHEMA_FILENAME=./sample.d/sample.avsc
export ENV_SCHEMA_FILENAME=./sample.d/output.avsc

export ENV_ENUM_COL_NAME=state

cat ./sample.d/sample.avro |
	./avro-enum2int |
	rq -aJ |
	jq -c
