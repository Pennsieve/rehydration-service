FROM golang:1.27.2-alpine

WORKDIR /usr/src/shared

COPY shared ./

WORKDIR /usr/src/app

COPY fargate ./
RUN go mod tidy


RUN go build -v -o /usr/local/bin/app .

CMD [ "app" ]