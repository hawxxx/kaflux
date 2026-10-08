# syntax=docker/dockerfile:1
# Base images are pinned by digest; Dependabot (docker ecosystem) keeps them current.

FROM --platform=$BUILDPLATFORM node:26-alpine@sha256:0b36e8c136b94cd4fcf02188228e76c31ad5872eef3fec8cbd2eee500cfd9e80 AS frontend
WORKDIR /src/frontend
RUN --mount=type=bind,source=frontend/package.json,target=package.json \
    --mount=type=bind,source=frontend/package-lock.json,target=package-lock.json \
    --mount=type=bind,source=frontend/.npmrc,target=.npmrc \
    --mount=type=cache,target=/root/.npm \
    npm ci
COPY frontend/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM golang:1.26.9-alpine@sha256:cdfd4fe2da6b225d8b40c6b7a105736e548e83ff56d5d8f9394446eeb5eb84e0 AS backend
ARG TARGETOS TARGETARCH
WORKDIR /src/backend
RUN --mount=type=bind,source=backend/go.mod,target=go.mod \
    --mount=type=bind,source=backend/go.sum,target=go.sum \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY backend/ ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/kaflux ./cmd/kaflux
RUN mkdir -p /out/data && chmod 0700 /out/data && chown 65532:65532 /out/data

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
LABEL org.opencontainers.image.source="https://github.com/hawxxx/kaflux" \
      org.opencontainers.image.licenses="MIT"
WORKDIR /app
COPY --link --from=backend /out/kaflux /app/kaflux
COPY --link --from=backend --chown=65532:65532 /out/data /var/lib/kaflux
COPY --link --from=frontend /src/frontend/dist /app/frontend
ENV KAFLUX_LISTEN=:8080 KAFLUX_STATIC_DIR=/app/frontend
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/app/kaflux"]
