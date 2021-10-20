################################################################
# STEP 1 use a temporary image to build a static monolith binary
################################################################
FROM golang:1.17  AS builder

# Pull build dependencies
ENV GOPATH=$APP_ROOT
ENV GOBIN=$APP_ROOT/bin
USER root


WORKDIR $GOPATH/src/
COPY . .

RUN go mod download
RUN echo "confluent.x14.se 10.1.1.90" >> /etc/hosts

# Build static image.

RUN go get -d github.com/alvaroloes/enumer && \
 go generate ./... && \ 
 GIT_SHA=$(git rev-parse --short HEAD) && \
 CGO_ENABLED=1 GOARCH=amd64 GOOS=linux go build -a \
 -ldflags "-extldflags '-static' -w -s -X main.appSha=$GIT_SHA" \
 -o /opt/app-root/src/github.com/ignalina/shredder/shredder \
 github.com/ignalina/shredder


FROM golang:1.17
USER root

RUN mkdir /app \
  && chown -R 1001:1001 /app


USER 1001
WORKDIR /opt/bin
COPY --from=builder /opt/app-root/src/github.com/ignalina/shredder/shredder /opt/bin/shredder

CMD /opt/bin/shredder
