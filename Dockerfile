FROM golang:latest

WORKDIR $GOPATH/src/KafkaScale
COPY . $GOPATH/src/KafkaScale

RUN go build .

EXPOSE 8093
ENTRYPOINT ["./KafkaScale"]
