/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.properties.scheme;

/**
 * Property Protection Schemes supported as arguments for encryption commands should not have direct references
 */
enum PropertyProtectionScheme implements ProtectionScheme {
    AES_GCM("aes/gcm"),

    AWS_SECRETSMANAGER("aws/secretsmanager"),

    AWS_KMS("aws/kms"),

    AZURE_KEYVAULT_KEY("azure/keyvault/key"),

    AZURE_KEYVAULT_SECRET("azure/keyvault/secret"),

    GCP_KMS("gcp/kms"),

    HASHICORP_VAULT_KV("hashicorp/vault/kv"),

    HASHICORP_VAULT_TRANSIT("hashicorp/vault/transit");

    PropertyProtectionScheme(final String path) {
        this.path = path;
    }

    private final String path;

    @Override
    public String getPath() {
        return path;
    }
}
