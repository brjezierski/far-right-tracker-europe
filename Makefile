setup:
	cd pipeline && uv sync
	cd frontend && npm install

data:  ## scrape everything (slow); COUNTRY=Germany for one country
	cd pipeline && uv run frt update $(COUNTRY)

summary:  ## rebuild JSON outputs from the CSVs, no scraping
	cd pipeline && uv run frt update --no-scraping $(COUNTRY)

dev: summary
	cd frontend && npm run dev

build: summary
	cd frontend && npm run build

lint:
	cd pipeline && uv run ruff check . && uv run ruff format --check .
	cd frontend && npm run typecheck && npm run lint

test:
	cd pipeline && uv run pytest

.PHONY: setup data summary dev build lint test
