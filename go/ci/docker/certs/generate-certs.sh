#!/bin/sh

# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generate the CA, server keystore, and client certificate/key used by the
# local Trino Docker Compose certificate authentication setup.

set -eu

CERT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CA_CONFIG="${CERT_DIR}/openssl-ca.cnf"
SERVER_CONFIG="${CERT_DIR}/openssl-server.cnf"
CLIENT_CONFIG="${CERT_DIR}/openssl-client.cnf"

CA_CERT="${CERT_DIR}/ca.crt"
KEYSTORE="${CERT_DIR}/localhost.p12"
CLIENT_CERT="${CERT_DIR}/client.crt"
CLIENT_KEY="${CERT_DIR}/client.key"

if [ -f "${CA_CERT}" ] && [ -f "${KEYSTORE}" ] && \
    [ -f "${CLIENT_CERT}" ] && [ -f "${CLIENT_KEY}" ] && \
    [ "${TRINO_FORCE_REGENERATE_CERTS:-0}" != "1" ]; then
  echo "Trino TLS assets already exist"
  exit 0
fi

if [ ! -f "${CA_CONFIG}" ] || [ ! -f "${SERVER_CONFIG}" ] || [ ! -f "${CLIENT_CONFIG}" ]; then
  echo "Missing OpenSSL config under ${CERT_DIR}" >&2
  exit 1
fi

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/trino-certs.XXXXXX")
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT INT TERM

run_openssl() {
  log_file="${TMP_DIR}/openssl.log"
  if ! openssl "$@" > /dev/null 2>"${log_file}"; then
    cat "${log_file}" >&2
    exit 1
  fi
}

CA_KEY="${TMP_DIR}/ca.key"
SERVER_KEY="${TMP_DIR}/localhost.key"
SERVER_CSR="${TMP_DIR}/localhost.csr"
CA_SERIAL="${TMP_DIR}/ca.srl"
TMP_CA_CERT="${TMP_DIR}/ca.crt"
TMP_SERVER_CERT="${TMP_DIR}/localhost.crt"
TMP_KEYSTORE="${TMP_DIR}/localhost.p12"
TMP_CLIENT_KEY="${TMP_DIR}/client.key"
CLIENT_CSR="${TMP_DIR}/client.csr"
TMP_CLIENT_CERT="${TMP_DIR}/client.crt"

run_openssl req \
  -x509 \
  -newkey rsa:2048 \
  -days 3650 \
  -nodes \
  -keyout "${CA_KEY}" \
  -out "${TMP_CA_CERT}" \
  -config "${CA_CONFIG}"

run_openssl req \
  -new \
  -newkey rsa:2048 \
  -nodes \
  -keyout "${SERVER_KEY}" \
  -out "${SERVER_CSR}" \
  -config "${SERVER_CONFIG}"

run_openssl x509 \
  -req \
  -days 3650 \
  -in "${SERVER_CSR}" \
  -CA "${TMP_CA_CERT}" \
  -CAkey "${CA_KEY}" \
  -CAcreateserial \
  -CAserial "${CA_SERIAL}" \
  -out "${TMP_SERVER_CERT}" \
  -extfile "${SERVER_CONFIG}" \
  -extensions v3_req

run_openssl pkcs12 \
  -export \
  -out "${TMP_KEYSTORE}" \
  -inkey "${SERVER_KEY}" \
  -in "${TMP_SERVER_CERT}" \
  -certfile "${TMP_CA_CERT}" \
  -name trino-dev \
  -passout pass:trino-dev

run_openssl req \
  -new \
  -newkey rsa:2048 \
  -nodes \
  -keyout "${TMP_CLIENT_KEY}" \
  -out "${CLIENT_CSR}" \
  -config "${CLIENT_CONFIG}"

run_openssl x509 \
  -req \
  -days 3650 \
  -in "${CLIENT_CSR}" \
  -CA "${TMP_CA_CERT}" \
  -CAkey "${CA_KEY}" \
  -CAserial "${CA_SERIAL}" \
  -out "${TMP_CLIENT_CERT}" \
  -extfile "${CLIENT_CONFIG}" \
  -extensions v3_req

cp "${TMP_CA_CERT}" "${CA_CERT}"
cp "${TMP_KEYSTORE}" "${KEYSTORE}"
cp "${TMP_CLIENT_CERT}" "${CLIENT_CERT}"
cp "${TMP_CLIENT_KEY}" "${CLIENT_KEY}"
# The generator runs as root in Docker; the host test process needs to read
# these generated test credentials too.
chmod 0644 "${CA_CERT}" "${KEYSTORE}" "${CLIENT_CERT}" "${CLIENT_KEY}"

echo "Generated Trino TLS assets in ${CERT_DIR}"
