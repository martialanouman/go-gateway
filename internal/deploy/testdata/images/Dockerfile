# Violates, on purpose: image-nonroot (a base with a shell, and no USER at all).
FROM alpine:3
ARG BINARY
COPY $BINARY /app
ENTRYPOINT ["/app"]
