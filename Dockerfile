FROM golang:1.23.4-alpine AS build-stage

WORKDIR /app

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/root/go-build go mod download -x

COPY . .

ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64

RUN go build -ldflags="-s -w" -o ./pgdump-cronjob

RUN apk add --no-cache postgresql-client

ENTRYPOINT ["/app/pgdump-cronjob"]
