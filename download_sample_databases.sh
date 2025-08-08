#!/bin/sh

echo "Downloading superheroes.db: ~1MB (used in stage 7)"
curl -Lo superheroes.db https://raw.githubusercontent.com/codecrafters-io/sample-sqlite-databases/master/superheroes.db


echo "Downloading samples.db: ~1MB (used in stage 1)"
curl -Lo sample.db https://raw.githubusercontent.com/codecrafters-io/sample-sqlite-databases/master/sample.db

echo "Downloading companies.db: ~7MB (used in stage 8)"
curl -Lo companies.db https://raw.githubusercontent.com/codecrafters-io/sample-sqlite-databases/master/companies.db

echo "Sample databases downloaded."
