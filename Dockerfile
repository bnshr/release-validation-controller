FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -o /controller .
FROM gcr.io/distroless/static:nonroot
COPY --from=build /controller /controller
ENTRYPOINT ["/controller"]
