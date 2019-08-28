#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

mkdir -p /runtime

openssl genrsa -out /runtime/root.key 4096
openssl req -x509 -new -nodes -sha256 -days 1024 -subj "/C=US/ST=AK/O=Apache NiFi/CN=localhost"  -key /runtime/root.key -out /runtime/root.crt
openssl genrsa -out /runtime/server.key 4096
openssl req -new -sha256 -key /runtime/server.key -subj "/C=US/ST=AK/O=Apache NiFi/CN=localhost" -reqexts SAN -config <(cat /etc/ssl/openssl.cnf <(printf "\n[SAN]\nsubjectAltName=DNS:localhost")) -out /runtime/server.csr
openssl x509 -req -in /runtime/server.csr -CA /runtime/root.crt -CAkey /runtime/root.key -CAcreateserial -out /runtime/server.crt -days 500 -sha256

chown vault:root /runtime/
chown vault:root /runtime/*

export VAULT_TLS_SERVER_NAME=localhost
export VAULT_LOCAL_CONFIG='{"backend":{"file":{"path":"/vault/file"}},"listener":{"tcp":{"address":"0.0.0.0:8300","tls_disable":0,"tls_cert_file":"/runtime/server.crt","tls_key_file":"/runtime/server.key"}},"default_lease_ttl":"168h","max_lease_ttl":"0h"}'

# Here we start Vault via the original image entrypoint script.  It will pick up the specific environment variables above,
# as well as any others passed in, such as VAULT_DEV_ROOT_TOKEN_ID.
/usr/local/bin/docker-entrypoint.sh server -dev
