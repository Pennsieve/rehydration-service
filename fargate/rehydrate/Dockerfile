FROM golang:1.21-alpine

WORKDIR /usr/src/app

COPY . .
RUN go mod tidy


RUN go build -v -o /usr/local/bin/app .

CMD [ "app" ]