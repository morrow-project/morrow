# syntax=docker/dockerfile:1.7

ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian13:nonroot

FROM alpine:3.22 AS fetch
ARG VERSION
ARG TARGETARCH

COPY docker-dist /releases

RUN set -eux; \
    archive="morrow-${VERSION}-linux-${TARGETARCH}.tar.gz"; \
    apk add --no-cache tar; \
    (cd /releases && sha256sum -c "${archive}.sha256"); \
    mkdir -p /out; \
    tar --extract --gzip --file "/releases/${archive}" --directory /tmp; \
    cp "/tmp/morrow-${VERSION}-linux-${TARGETARCH}/morrow-server" /out/morrow-server; \
    chmod 0555 /out/morrow-server

FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /var/lib/morrow
COPY --from=fetch /out/morrow-server /usr/local/bin/morrow-server
USER 65532:65532
EXPOSE 4222
VOLUME ["/var/lib/morrow"]
ENTRYPOINT ["/usr/local/bin/morrow-server"]
