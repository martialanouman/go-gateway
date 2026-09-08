# syntax=docker/dockerfile:1
#
# The service images: eleven images that differ only by the binary they carry, so they share one file.
# Eleven copies would be eleven places the USER line can quietly disappear — and deploy/k8s sets no
# securityContext, so this line is the ONLY thing standing between the workload and running as root.
#
# Nothing is COMPILED here. GoReleaser has already produced the static binary (CGO_ENABLED=0), and the
# build context is the temporary directory dockers_v2 stages, where artefacts live under
# <goos>/<goarch>/<binary> — hence $TARGETPLATFORM, which BuildKit fills in on its own.
#
# static-debian12 rather than scratch: it carries the three things a static Go binary still expects
# from a filesystem — the root CAs (OTLP over TLS, Postgres sslmode=verify-full in step-300),
# /etc/passwd so uid 65532 resolves, and tzdata. No shell, no package manager, no libc: the surface
# stays the one ADR-0011 demands of content-key-svc, which holds the KEK.
FROM gcr.io/distroless/static-debian12:nonroot

ARG TARGETPLATFORM
ARG BINARY

# Numeric, not "nonroot": the kubelet refuses a pod with runAsNonRoot whose image carries a user it
# cannot resolve to a number. 65532 is the uid of the distroless nonroot variant.
USER 65532:65532

# The binary stays owned by root and read-only to uid 65532 — no --chown, deliberately: a compromised
# process cannot rewrite its own executable.
#
# Renamed to /app because ENTRYPOINT in exec form interpolates no ARG, and the shell form does not
# exist in an image with no shell. The service name still reaches the logs (each main's serviceName
# constant) and the OCI title label.
COPY $TARGETPLATFORM/$BINARY /app

ENTRYPOINT ["/app"]
