# Build stage
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/server .

FROM alpine:latest
WORKDIR /app
COPY --from=build /bin/server .
COPY api ./api
EXPOSE 8080
ENTRYPOINT ["./server"]
