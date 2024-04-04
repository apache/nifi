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
package org.apache.nifi.cdc.mysql.processors.ssl;

/**
 * MySQL Connector/J Security Properties
 */
public enum SecurityProperty {
    /** Deprecated alias for tlsVersions */
    ENABLED_TLS_PROTOCOLS("enabledTLSProtocols"),

    /** Added in MySQL 5.1.0 */
    TRUST_CERTIFICATE_KEY_STORE_URL("trustsCertificateKeyStoreUrl"),

    /** Added in MySQL 5.1.0 and defaults to JKS */
    TRUST_CERTIFICATE_KEY_STORE_TYPE("trustCertificateKeyStoreType"),

    /** Added in MySQL 5.1.0 */
    TRUST_CERTIFICATE_KEY_STORE_PASSWORD("trustCertificateKeyStorePassword"),

    /** Added in MySQL 5.1.0 */
    CLIENT_CERTIFICATE_KEY_STORE_URL("clientCertificateKeyStoreUrl"),

    /** Added in MySQL 5.1.0 and defaults to JKS */
    CLIENT_CERTIFICATE_KEY_STORE_TYPE("clientCertificateKeyStoreType"),

    /** Added in MySQL 5.1.0 */
    CLIENT_CERTIFICATE_KEY_STORE_PASSWORD("clientCertificateKeyStorePassword"),

    /** Deprecated in favor of sslMode and evaluated when useSSL is enabled */
    REQUIRE_SSL("requireSSL"),

    /** Deprecated in favor of sslMode and defaults to true in 8.0.13 and later */
    USE_SSL("useSSL"),

    /** Deprecated in favor of sslMode and defaults to false in 8.0.13 and later */
    VERIFY_SERVER_CERTIFICATE("verifyServerCertificate");

    private final String property;

    SecurityProperty(final String property) {
        this.property = property;
    }

    public String getProperty() {
        return property;
    }
}
