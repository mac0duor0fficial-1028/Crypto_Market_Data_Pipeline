.RECIPEPREFIX := >
.PHONY: up down reset logs ps test verify

up:
> docker compose up -d --build

down:
> docker compose down

reset:
> docker compose down -v

logs:
> docker compose logs -f --tail=100

ps:
> docker compose ps

test:
> pip install -q -r requirements-dev.txt && pytest -q

verify:
> docker compose run --rm api python /app/scripts/verify_restart.py --symbol BTC/USD --minutes 60 --lag-minutes 15
