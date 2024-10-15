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
package org.apache.nifi.ssl;

import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsPlatform;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Standard implementation of TLS Configuration for SSL Context Services
 */
class StandardTlsConfiguration implements TlsConfiguration {
    protected static final String SSL_PROTOCOL = "SSL";
    protected static final String TLS_PROTOCOL = "TLS";

    private static final String MASKED_PASSWORD_LOG = "********";
    private static final String NULL_LOG = "null";

    private final String keystorePath;
    private final String keystorePassword;
    private final String keyPassword;
    private final KeystoreType keystoreType;

    private final String truststorePath;
    private final String truststorePassword;
    private final KeystoreType truststoreType;

    private final String protocol;

    /**
     * Instantiates a container object with the given configuration values.
     *
     * @param keystorePath       the keystore path
     * @param keystorePassword   the keystore password
     * @param keyPassword        the (optional) key password -- if {@code null}, the keystore password is assumed the same for the individual key
     * @param keystoreType       the keystore type as a String
     * @param truststorePath     the truststore path
     * @param truststorePassword the truststore password
     * @param truststoreType     the truststore type as a String
     * @param protocol           the TLS protocol version string
     */
    public StandardTlsConfiguration(String keystorePath, String keystorePassword, String keyPassword,
                            String keystoreType, String truststorePath, String truststorePassword, String truststoreType, String protocol) {
        this(keystorePath, keystorePassword, keyPassword,
                (KeystoreType.isValidKeystoreType(keystoreType) ? KeystoreType.valueOf(keystoreType.toUpperCase()) : null),
                truststorePath, truststorePassword,
                (KeystoreType.isValidKeystoreType(truststoreType) ? KeystoreType.valueOf(truststoreType.toUpperCase()) : null),
                protocol);
    }

    /**
     * Instantiates a container object with the given configuration values.
     *
     * @param keystorePath       the keystore path
     * @param keystorePassword   the keystore password
     * @param keyPassword        the (optional) key password -- if {@code null}, the keystore password is assumed the same for the individual key
     * @param keystoreType       the keystore type
     * @param truststorePath     the truststore path
     * @param truststorePassword the truststore password
     * @param truststoreType     the truststore type
     * @param protocol           the TLS protocol version string
     */
    public StandardTlsConfiguration(String keystorePath, String keystorePassword, String keyPassword,
                            KeystoreType keystoreType, String truststorePath, String truststorePassword, KeystoreType truststoreType, String protocol) {
        this.keystorePath = keystorePath;
        this.keystorePassword = keystorePassword;
        this.keyPassword = keyPassword;
        this.keystoreType = keystoreType;
        this.truststorePath = truststorePath;
        this.truststorePassword = truststorePassword;
        this.truststoreType = truststoreType;
        this.protocol = protocol;
    }

    @Override
    public String getKeystorePath() {
        return keystorePath;
    }

    @Override
    public String getKeystorePassword() {
        return keystorePassword;
    }

    /**
     * Returns {@code "********"} if the keystore password is populated, {@code "null"} if not.
     *
     * @return a loggable String representation of the keystore password
     */
    @Override
    public String getKeystorePasswordForLogging() {
        return maskPasswordForLog(keystorePassword);
    }

    @Override
    public String getKeyPassword() {
        return keyPassword;
    }

    /**
     * Returns {@code "********"} if the key password is populated, {@code "null"} if not.
     *
     * @return a loggable String representation of the key password
     */
    @Override
    public String getKeyPasswordForLogging() {
        return maskPasswordForLog(keyPassword);
    }

    /**
     * Returns the "working" key password -- if the key password is populated, it is returned; otherwise the {@link #getKeystorePassword()} is returned.
     *
     * @return the key or keystore password actually populated
     */
    @Override
    public String getFunctionalKeyPassword() {
        return isNotBlank(keyPassword) ? keyPassword : keystorePassword;
    }

    /**
     * Returns {@code "********"} if the functional key password is populated, {@code "null"} if not.
     *
     * @return a loggable String representation of the functional key password
     */
    @Override
    public String getFunctionalKeyPasswordForLogging() {
        return maskPasswordForLog(getFunctionalKeyPassword());
    }

    @Override
    public KeystoreType getKeystoreType() {
        return keystoreType;
    }

    @Override
    public String getTruststorePath() {
        return truststorePath;
    }

    @Override
    public String getTruststorePassword() {
        return truststorePassword;
    }

    /**
     * Returns {@code "********"} if the truststore password is populated, {@code "null"} if not.
     *
     * @return a loggable String representation of the truststore password
     */
    @Override
    public String getTruststorePasswordForLogging() {
        return maskPasswordForLog(truststorePassword);
    }

    @Override
    public KeystoreType getTruststoreType() {
        return truststoreType;
    }

    @Override
    public String getProtocol() {
        return protocol;
    }

    // Boolean validators for keystore & truststore

    /**
     * Returns {@code true} if the necessary properties are populated to instantiate a <strong>keystore</strong>. This does <em>not</em> validate the values (see {@link #isKeystoreValid()}).
     *
     * @return true if the path, password, and type are present
     */
    @Override
    public boolean isKeystorePopulated() {
        return isStorePopulated(keystorePath, keystorePassword, keystoreType, StoreType.KEY_STORE);
    }

    /**
     * Returns {@code true} if <em>any</em> of the keystore properties is populated, indicating that the caller expects a valid keystore to be generated.
     *
     * @return true if any keystore properties are present
     */
    @Override
    public boolean isAnyKeystorePopulated() {
        return isAnyPopulated(keystorePath, keystorePassword, keystoreType);
    }

    /**
     * Returns {@code true} if the necessary properties are populated and the keystore can be successfully instantiated (i.e. the path is valid and the password(s) are correct).
     *
     * @return true if the keystore properties are valid
     */
    @Override
    public boolean isKeystoreValid() {
        return isStoreValid(keystorePath, keystorePassword, keystoreType, StoreType.KEY_STORE);
    }

    /**
     * Returns {@code true} if the necessary properties are populated to instantiate a <strong>truststore</strong>. This does <em>not</em> validate the values (see {@link #isTruststoreValid()}).
     *
     * @return true if the path, password, and type are present
     */
    @Override
    public boolean isTruststorePopulated() {
        return isStorePopulated(truststorePath, truststorePassword, truststoreType, StoreType.TRUST_STORE);
    }

    /**
     * Returns {@code true} if <em>any</em> of the truststore properties is populated, indicating that the caller expects a valid truststore to be generated.
     *
     * @return true if any truststore properties are present
     */
    @Override
    public boolean isAnyTruststorePopulated() {
        return isAnyPopulated(truststorePath, truststorePassword, truststoreType);
    }

    /**
     * Returns {@code true} if the necessary properties are populated and the truststore can be successfully instantiated (i.e. the path is valid and the password is correct).
     *
     * @return true if the truststore properties are valid
     */
    @Override
    public boolean isTruststoreValid() {
        return isStoreValid(truststorePath, truststorePassword, truststoreType, StoreType.TRUST_STORE);
    }

    /**
     * Returns a {@code String[]} containing the keystore properties for logging. The order is
     * {@link #getKeystorePath()}, {@link #getKeystorePasswordForLogging()},
     * {@link #getFunctionalKeyPasswordForLogging()}, {@link #getKeystoreType()} (using the type or "null").
     *
     * @return a loggable String[]
     */
    @Override
    public String[] getKeystorePropertiesForLogging() {
        return new String[]{getKeystorePath(), getKeystorePasswordForLogging(), getFunctionalKeyPasswordForLogging(), getKeystoreType() != null ? getKeystoreType().getType() : NULL_LOG};
    }

    /**
     * Returns a {@code String[]} containing the truststore properties for logging. The order is
     * {@link #getTruststorePath()}, {@link #getTruststorePasswordForLogging()},
     * {@link #getTruststoreType()} (using the type or "null").
     *
     * @return a loggable String[]
     */
    @Override
    public String[] getTruststorePropertiesForLogging() {
        return new String[]{getTruststorePath(), getTruststorePasswordForLogging(), getKeystoreType() != null ? getTruststoreType().getType() : NULL_LOG};
    }


    /**
     * Get Enabled TLS Protocols translates SSL to legacy protocols and TLS to current protocols or returns configured protocol
     *
     * @return Enabled TLS Protocols
     */
    @Override
    public String[] getEnabledProtocols() {
        final List<String> enabledProtocols = new ArrayList<>();

        final String configuredProtocol = getProtocol();
        if (TLS_PROTOCOL.equals(configuredProtocol)) {
            enabledProtocols.addAll(TlsPlatform.getPreferredProtocols());
        } else if (SSL_PROTOCOL.equals(configuredProtocol)) {
            enabledProtocols.addAll(TlsPlatform.getSupportedProtocols());
        } else if (configuredProtocol != null) {
            enabledProtocols.add(configuredProtocol);
        }

        return enabledProtocols.toArray(new String[0]);
    }

    @Override
    public String toString() {
        return "[TlsConfiguration]" + "keystorePath=" + keystorePath +
                ",keystorePassword=" + getKeystorePasswordForLogging() +
                ",keyPassword=" + getKeyPasswordForLogging() +
                ",keystoreType=" + keystoreType +
                ",truststorePath=" + truststorePath +
                ",truststorePassword=" + getTruststorePasswordForLogging() +
                ",truststoreType=" + truststoreType +
                ",protocol=" + protocol;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        org.apache.nifi.security.util.TlsConfiguration that = (org.apache.nifi.security.util.TlsConfiguration) o;
        return Objects.equals(keystorePath, that.getKeystorePath())
                && Objects.equals(keystorePassword, that.getKeystorePassword())
                && Objects.equals(keyPassword, that.getKeyPassword())
                && keystoreType == that.getKeystoreType()
                && Objects.equals(truststorePath, that.getTruststorePath())
                && Objects.equals(truststorePassword, that.getTruststorePassword())
                && truststoreType == that.getTruststoreType()
                && Objects.equals(protocol, that.getProtocol());
    }

    @Override
    public int hashCode() {
        return Objects.hash(keystorePath, keystorePassword, keyPassword, keystoreType, truststorePath, truststorePassword, truststoreType, protocol);
    }

    private static String maskPasswordForLog(String password) {
        return isNotBlank(password) ? MASKED_PASSWORD_LOG : NULL_LOG;
    }

    private boolean isAnyPopulated(String path, String password, KeystoreType type) {
        return isNotBlank(path) || isNotBlank(password) || type != null;
    }

    private boolean isStorePopulated(final String path, final String password, final KeystoreType type, final StoreType storeType) {
        boolean populated;

        // Legacy trust stores such as JKS can be populated without a password; only check the path and type
        populated = isNotBlank(path) && type != null;
        if (StoreType.KEY_STORE == storeType) {
            populated = populated && isNotBlank(password);
        }

        return populated;
    }

    private boolean isStoreValid(final String path, final String password, final KeystoreType type, final StoreType storeType) {
        return isStorePopulated(path, password, type, storeType) && isStoreValid(path, password.toCharArray(), type.getType());
    }

    private boolean isStoreValid(final String path, final char[] password, final String type) {
        try (InputStream inputStream = new FileInputStream(path)) {
            new StandardKeyStoreBuilder()
                    .inputStream(inputStream)
                    .password(password)
                    .type(type)
                    .build();
            return true;
        } catch (final Exception e) {
            return false;
        }
    }

    private static boolean isNotBlank(final String string) {
        return string != null && !string.isBlank();
    }

    private enum StoreType {
        KEY_STORE,
        TRUST_STORE
    }
}
