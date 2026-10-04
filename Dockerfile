FROM node:22-alpine AS frontend
WORKDIR /src/frontend
COPY frontend/.npmrc frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM golang:1.26.8-alpine AS backend
WORKDIR /src/backend
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kaflux ./cmd/kaflux
RUN mkdir -p /out/data && chmod 0700 /out/data && chown 65532:65532 /out/data

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=backend /out/kaflux /app/kaflux
COPY --from=backend --chown=65532:65532 /out/data /var/lib/kaflux
COPY --from=frontend /src/frontend/dist /app/frontend
ENV KAFLUX_LISTEN=:8080 KAFLUX_STATIC_DIR=/app/frontend
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/app/kaflux"]
