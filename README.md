# event_pusher

POC exploring how Kafka could be replaced by **NATS JetStream** and **Redis Streams**, with **S3** (MinIO locally) for object storage.

## Requirements

- Go 1.25+
- Docker with Compose v2

## Quick start

```sh
make up      # build the image, start infra + producer + service
make logs    # follow producer and service logs
make down    # stop (data kept); `make reset` also wipes volumes
```

To run the Go code on your machine against the dockerized infra:

```sh
make infra-up
make run-producer   # or: make run-service
```

## Commands

One binary, two subcommands:

| Command                 | Role                       |
|-------------------------|----------------------------|
| `event_pusher producer` | produces events            |
| `event_pusher service`  | runs the event pusher service |

`event_pusher <command> --help` lists every flag.

## Configuration

Each setting can come from (highest priority first):

1. a CLI flag: `--Nats.URL=nats://host:4222`
2. an env var: `EVENT_PUSHER_` + key, `.` replaced by `_` (e.g. `EVENT_PUSHER_NATS_URL`)
3. a config file: `--config=config.yaml` (json, yaml, toml...)
4. the built-in default (targets the local compose)

`producer` only reads `LogLevel` and `Nats.*`; `service` reads all keys.

| Key              | Default                 |
|------------------|-------------------------|
| `LogLevel`       | `info` (debug, info, warn, error) |
| `Nats.URL`       | `nats://localhost:4222` |
| `Redis.Addr`     | `localhost:6379`        |
| `Redis.Password` | *(empty)*               |
| `Redis.DB`       | `0`                     |
| `Redis.BatchSize` | `1000` (entries read or deleted per command when emptying a stream) |
| `S3.Endpoint`    | `localhost:9000`        |
| `S3.Region`      | `us-east-1`             |
| `S3.Bucket`      | `events`                |
| `S3.AccessKey`   | *(empty)*               |
| `S3.SecretKey`   | *(empty)*               |
| `S3.UseSSL`      | `false`                 |

In the compose stack, each service reads its dev config file, mounted read-only and passed with `--config`:
[config/dev/producer.yaml](config/dev/producer.yaml) and [config/dev/event_pusher.yaml](config/dev/event_pusher.yaml). Edit one and run `docker compose up -d` to apply it. The hosts there are compose service names; to run on your machine, use `make run-*` (defaults target `localhost`) or `--config` with your own file.

Config file example:

```yaml
LogLevel: debug
Nats:
  URL: nats://localhost:4222
S3:
  AccessKey: minioadmin
  SecretKey: minioadmin
```

## Local infrastructure

| Service | Ports | Notes |
|---------|-------|-------|
| NATS (JetStream on) | `4222` clients, `8222` monitoring | JetStream status: <http://localhost:8222/jsz> |
| Redis 8 | `6379` | streams built in, AOF persistence |
| MinIO | `9000` S3 API, `9001` console | `minioadmin` / `minioadmin`, console: <http://localhost:9001> |

MinIO uses the `pgsty/minio` community fork: the official images are no longer pullable.

## Docker image

```sh
make docker-build                          # via compose, tags event_pusher:local
docker build -t event_pusher:local .       # plain docker
docker run --rm event_pusher:local service --help
```

Multi-stage build: static binary (`CGO_ENABLED=0`) on `distroless/static:nonroot`, about 13 MB.

## Makefile

`make help` lists every target. The ones you'll use most:

| Target | What it does |
|--------|--------------|
| `up` / `down` / `reset` | start / stop / stop and wipe volumes for the full stack |
| `infra-up` | start NATS, Redis and MinIO only |
| `logs` / `ps` | follow app logs / container status |
| `run-producer` / `run-service` | run a command locally with `go run` |
| `build` | build `bin/event_pusher` |
| `test` / `cover` | unit tests (race detector) / coverage per package |
| `fmt` / `vet` / `tidy` | format / static analysis / clean go.mod |

## Layout

```
main.go                  entry point, signal handling
cmd/                     cobra commands (thin: flags, config load, run)
  producer/  event_pusher/
config/dev/              dev config files mounted in the compose services
internal/
  config/                per-command config + nats, redis, s3 blocks
    confighelper/        flag prefixing, viper loading (flags > env > file)
  producer/              producer app
  event_pusher/          service app
  logger/                slog JSON logger
pkg/model/               event types shared by both commands
```

To add a setting: add the field (with its `mapstructure` tag) to the relevant `Config`, its default to `default_config.go`, and its flag to `FlagSet`. Env var and config file support come for free.
