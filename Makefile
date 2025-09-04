dev:
	python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
infra-up:
	docker compose up -d
infra-down:
	docker compose down -v
run:
	python -m tdg.cli generate --out ./out
seed:
	python -m tdg.cli seed-postgres
api:
	python -m tdg.cli call-api --users 10 --orders 20 --reviews 20
test:
	python -m tdg.tests.smoke
