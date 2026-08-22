#!/bin/sh
set -eu

secret_dir=${1:-.secrets}
umask 077
mkdir -p "$secret_dir"

test -s "$secret_dir/morrow-admin-token" || openssl rand -hex 32 > "$secret_dir/morrow-admin-token"
test -s "$secret_dir/morrow-cluster-token" || openssl rand -hex 32 > "$secret_dir/morrow-cluster-token"

if test ! -s "$secret_dir/morrow-client-public-key"; then
  openssl genpkey -algorithm ED25519 -out "$secret_dir/local-client-key.pem"
  openssl pkey -in "$secret_dir/local-client-key.pem" -outform DER \
    | tail -c 32 | xxd -p -c 64 > "$secret_dir/local-client-seed"
  openssl pkey -in "$secret_dir/local-client-key.pem" -pubout -outform DER \
    | tail -c 32 | xxd -p -c 64 > "$secret_dir/morrow-client-public-key"
fi

if test -e "$secret_dir/morrow-ca-cert.pem"; then
  echo "TLS material already exists in $secret_dir; refusing to overwrite it" >&2
  exit 0
fi

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM
openssl ecparam -genkey -name prime256v1 -out "$work_dir/ca-key.pem"
openssl req -x509 -new -key "$work_dir/ca-key.pem" -days 3650 \
  -subj "/CN=morrow-compose-ca" -addext "basicConstraints=critical,CA:TRUE" \
  -out "$work_dir/ca-cert.pem"

node=1
while test "$node" -le 3; do
  openssl ecparam -genkey -name prime256v1 -out "$work_dir/node-$node-key.pem"
  openssl req -new -key "$work_dir/node-$node-key.pem" \
    -subj "/CN=morrow-$node" -out "$work_dir/node-$node.csr"
  printf 'subjectAltName=DNS:morrow-%s,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n' \
    "$node" > "$work_dir/node-$node.ext"
  openssl x509 -req -in "$work_dir/node-$node.csr" \
    -CA "$work_dir/ca-cert.pem" -CAkey "$work_dir/ca-key.pem" -CAcreateserial \
    -days 825 -extfile "$work_dir/node-$node.ext" \
    -out "$work_dir/node-$node-cert.pem"
  mv "$work_dir/node-$node-key.pem" "$secret_dir/morrow-node-$node-key.pem"
  mv "$work_dir/node-$node-cert.pem" "$secret_dir/morrow-node-$node-cert.pem"
  node=$((node + 1))
done
mv "$work_dir/ca-cert.pem" "$secret_dir/morrow-ca-cert.pem"

echo "Generated Compose credentials in $secret_dir"
