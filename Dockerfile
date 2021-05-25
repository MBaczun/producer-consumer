FROM amd64/golang:1.15.7-alpine3.13 as consumer

WORKDIR /app
COPY . .
CMD ["go", "run", "./consumer/consumer.go"]

FROM amd64/golang:1.15.7-alpine3.13 as producer

WORKDIR /app
COPY . .
CMD ["go", "run", "./producer/producer.go"]