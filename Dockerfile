FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/job-recovery ./cmd/job-recovery
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/job-recovery /job-recovery
WORKDIR /app
COPY migrations /app/migrations
ENTRYPOINT ["/job-recovery"]
