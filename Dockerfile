FROM ruby:3.0.6 as geminstall

RUN apt update && apt install -y \
        build-essential \
        curl

COPY --from=golang:1.20 /usr/local/go/ /usr/local/go/
ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

COPY . /src
WORKDIR /src
RUN cd ext && go build -buildmode=c-shared -o ruby_snowflake_client.so ruby_snowflake.go

RUN rake build

CMD ["bash"]
