ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

# Dependencies in their own layer: only rebuilt when go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/event_pusher .

# Static binary on distroless: no shell, non-root user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/event_pusher /event_pusher
ENTRYPOINT ["/event_pusher"]
