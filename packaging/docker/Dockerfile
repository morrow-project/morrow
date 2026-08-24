# syntax=docker/dockerfile:1.7

ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian13:nonroot

FROM alpine:3.22 AS fetch
ARG VERSION
ARG TARGETARCH

RUN set -eux; \
    archive="morrow-${VERSION}-linux-${TARGETARCH}.tar.gz"; \
    base_url="https://github.com/morrow-project/morrow/releases/download/${VERSION}"; \
    apk add --no-cache curl tar; \
    curl --fail --silent --show-error --location "${base_url}/${archive}" --output "/tmp/${archive}"; \
    curl --fail --silent --show-error --location "${base_url}/${archive}.sha256" --output "/tmp/${archive}.sha256"; \
    (cd /tmp && sha256sum -c "${archive}.sha256"); \
    mkdir -p /out; \
    tar --extract --gzip --file "/tmp/${archive}" --directory /tmp; \
    cp "/tmp/morrow-${VERSION}-linux-${TARGETARCH}/morrow-server" /out/morrow-server; \
    chmod 0555 /out/morrow-server

FROM ${RUNTIME_IMAGE} AS runtime
WORKDIR /var/lib/morrow
COPY --from=fetch /out/morrow-server /usr/local/bin/morrow-server
USER 65532:65532
EXPOSE 4222
VOLUME ["/var/lib/morrow"]
ENTRYPOINT ["/usr/local/bin/morrow-server"]
