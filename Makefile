.PHONY: help build run bench up down migrate seed demo verify test test-unit test-integration test-race lint logs reset chaos-demo
help:
	@printf '%s\n' 'build run up down migrate seed demo verify test test-integration test-race lint bench logs reset chaos-demo'
build:
	go build ./cmd/job-recovery
run:
	@test -f .env || (echo 'Copy .env.example to .env and set local credentials' >&2; exit 1)
	@set -a; . ./.env; set +a; go run ./cmd/job-recovery serve
bench:
	@echo "No benchmark fixtures are implemented yet" >&2; exit 1
up:
	docker compose up --build -d

down:
	docker compose down
migrate: up
	docker compose run --rm migrate
seed:
	@test -f .env || (echo 'Copy .env.example to .env and set local credentials' >&2; exit 1)
	@set -a; . ./.env; set +a; curl -fsS -X POST http://127.0.0.1:8080/api/demo/seed -H 'Content-Type: application/json' -H "Authorization: Bearer $$ADMIN_TOKEN" -d '{"count":100}'
demo: up
	@set -a; . ./.env; set +a; go run ./cmd/job-recovery demo seed --count 100
verify:
	test -n "$(RUN_ID)" && curl -fsS "http://127.0.0.1:8080/api/demo/verify?run_id=$(RUN_ID)"
test test-unit:
	go test ./...
test-race:
	go test -race ./...
test-integration:
	docker compose up -d --wait postgres nats
	@test -f .env || (echo 'Copy .env.example to .env and set local credentials' >&2; exit 1)
	@set -a; . ./.env; set +a; REQUIRE_INTEGRATION=1 go test -run Integration ./internal
lint:
	test -z "$(gofmt -l cmd internal)"
	go vet ./...
logs:
	docker compose logs -f
reset:
	docker compose down -v
chaos-demo:
	./scripts/chaos-demo.sh
