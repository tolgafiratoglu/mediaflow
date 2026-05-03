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
	@find proto -name "*.proto" -not -path "*/vendor/*" -exec \
		protoc \
		--proto_path=proto \
		--go_out=proto \
		--go_opt=paths=source_relative \
		--go-grpc_out=proto \
		--go-grpc_opt=paths=source_relative \
		{} \;

tidy:
	@for dir in proto services/*/; do \
		echo "→ $$dir"; \
		cd $$dir && go mod tidy && cd ../..; \
	done
