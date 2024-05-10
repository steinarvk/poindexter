#!/bin/bash
rm -rf postgres_testdata
mkdir postgres_testdata
docker compose --file docker-compose.yml down
docker compose --verbose --file docker-compose.yml up --detach
export POSTGRES_CONNECTION_STRING="postgresql://poindexter_test:testpassword@localhost:15433/poindexter_test?sslmode=disable"
echo "Waiting for database ready"
sleep 10s
echo "Done waiting for database ready"
migrate -path  ../../../lib/poindexterdb/migrations -database "${POSTGRES_CONNECTION_STRING}" down --all
migrate -path  ../../../lib/poindexterdb/migrations -database "${POSTGRES_CONNECTION_STRING}" up
