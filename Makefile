SHELL := bash
# source ./datrang-ingestor/.env


init:
	docker compose up -d
	POSTGRES__POST="localhost" && cargo run --package datrang-ingestor migrate
	PGPASSWORD="postgres" psql -v ON_ERROR_STOP=1 -h "$$POSTGRES__HOST" \
		-p "$$POSTGRES__PORT" -U "$$POSTGRES__USERNAME"  -d "$$POSTGRES__NAME" -c "$$(cat sample.sql)"

start:
	cargo run --package datrang-ingestor

insert_records:
	PGPASSWORD="postgres" psql -v ON_ERROR_STOP=1 -h "$$POSTGRES__HOST" \
		-p "$$POSTGRES__PORT" -U "$$POSTGRES__USERNAME"  -d "$$POSTGRES__NAME" -c "$$(cat insert.sql)"

clean:
	docker compose down
	docker volume rm datrang_pg_data