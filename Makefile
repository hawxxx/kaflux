.PHONY: test build dev docker helm
test:
	cd backend && go test -race ./... && go vet ./...
	cd frontend && npm ci && npm test -- --run && npm run build
build:
	cd backend && go build ./cmd/kaflux
	cd frontend && npm ci && npm run build
dev:
	cd backend && KAFLUX_DEMO=true go run ./cmd/kaflux
docker:
	docker build -t kaflux:local .
helm:
	helm lint deploy/helm/kaflux
	helm template kaflux deploy/helm/kaflux >/dev/null
