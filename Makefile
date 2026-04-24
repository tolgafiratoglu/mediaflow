.PHONY: up down build logs ps proto

up:
	docker compose up --build -d

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f

ps:
	docker compose ps

proto:
	@echo "Generating proto files..."
	@find proto -name "*.proto" -exec \
		protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		{} \;

tidy:
	@for dir in services/*/; do \
		echo "→ $$dir"; \
		cd $$dir && go mod tidy && cd ../..; \
	done
