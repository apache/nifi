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
package org.apache.nifi.security.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a container object implementing {@link SSLConfig} for passing common TLS/SSL configuration values
 * around between extensions (components) and the core framework and security utilities which generate the internal
 * JCA/JCE classes to create the secure connections.
 */
public class StandardSSLConfig implements SSLConfig {
    private static final Logger logger = LoggerFactory.getLogger(StandardSSLConfig.class);

    private String alias;

    private String keystorePath;
    private String keystorePassword;
    private String keyPassword;
    private KeystoreType keystoreType;

    private String truststorePath;
    private String truststorePassword;
    private KeystoreType truststoreType;

    private SslContextFactory.ClientAuth clientAuth;
    private String protocol;

    public StandardSSLConfig() {
    }

    // TODO: Implement Builder and constructor

    /**
     * Returns the keystore alias to use.
     *
     * @return the alias
     */
    @Override
    public String getAlias() {
        return alias;
    }

    /**
     * Sets the keystore alias to use. If none is provided, the first suitable alias encountered will be used. Aliases
     * are trimmed for leading and trailing spaces.
     *
     * @param alias the alias identifier
     */
    @Override
    public void setAlias(String alias) {
        if (alias == null || alias.trim().length() == 0) {
            this.alias = null;
        } else {
            this.alias = alias.trim();
        }
    }

    /**
     * Returns the keystore path. Absolute paths are preferred.
     *
     * @return the keystore path
     */
    @Override
    public String getKeystorePath() {
        return keystorePath;
    }

    /**
     * Sets the keystore path. Absolute paths are preferred. Paths are trimmed for leading and trailing spaces.
     *
     * @param keystorePath the file path to the keystore resource
     */
    @Override
    public void setKeystorePath(String keystorePath) {
        if (keystorePath == null || keystorePath.trim().length() == 0) {
            this.keystorePath = null;
        } else {
            this.keystorePath = keystorePath.trim();
        }
    }

    /**
     * Returns the keystore password.
     *
     * @return the keystore password
     */
    @Override
    public String getKeystorePassword() {
        return keystorePassword;
    }

    /**
     * Returns the keystore password.
     *
     * @return the keystore password
     */
    @Override
    public char[] getKeystorePasswordAsChars() {
        return keystorePassword == null ? new char[0] : keystorePassword.toCharArray();
    }

    /**
     * Sets the keystore password. Leading and trailing spaces are trimmed.
     *
     * @param keystorePassword the keystore password
     */
    @Override
    public void setKeystorePassword(String keystorePassword) {
        // TODO: Add warn logging for null passwords?
        if (keystorePassword == null || keystorePassword.trim().length() == 0) {
            this.keystorePassword = null;
        } else {
            this.keystorePassword = keystorePassword.trim();
        }
    }

    /**
     * Sets the keystore password. Leading and trailing spaces are trimmed.
     *
     * @param keystorePassword the keystore password
     */
    @Override
    public void setKeystorePassword(char[] keystorePassword) {
        setKeystorePassword(String.valueOf(keystorePassword));
    }

    /**
     * Returns the key password. If none is set, {@code null} is returned.
     * <p>
     * See also {@link #getKeyOrKeystorePassword()} and {@link #isUniqueKeyPasswordSet()}.
     *
     * @return the key password
     */
    @Override
    public String getKeyPassword() {
        return keyPassword;
    }

    /**
     * Returns the key password.
     *
     * @return the key password
     */
    @Override
    public char[] getKeyPasswordAsChars() {
        return keyPassword == null ? new char[0] : keyPassword.toCharArray();
    }

    /**
     * Sets the key password. Accepts {@code null} input. Leading and trailing spaces are trimmed.
     *
     * @param keyPassword the key password
     */
    @Override
    public void setKeyPassword(String keyPassword) {
        if (keyPassword == null || keyPassword.trim().length() == 0) {
            this.keyPassword = null;
        } else {
            this.keyPassword = keyPassword.trim();
        }
    }

    /**
     * Sets the key password. Accepts {@code null} input. Leading and trailing spaces are trimmed.
     *
     * @param keyPassword the key password
     */
    @Override
    public void setKeyPassword(char[] keyPassword) {
        // TODO: Check if identical to existing keystore password and skip?
        setKeyPassword(String.valueOf(keyPassword));
    }

    /**
     * Returns {@code true} if a password value is set for the <em>key</em> which is different from the <em>keystore</em>
     * password. If the key password is {@code null} or identical to the keystore password, returns {@code false}.
     *
     * @return true if a unique key password is set
     */
    @Override
    public boolean isUniqueKeyPasswordSet() {
        return keyPassword != null && !(keyPassword.equals(keystorePassword));
    }

    /**
     * Returns the password for the key being used -- this can be the <em>key</em> password if it is set, or the
     * <em>keystore</em> password if no key password is set. Either way, this value will be accepted by the keystore
     * when accessing the key.
     *
     * @return the key or keystore password
     */
    @Override
    public String getKeyOrKeystorePassword() {
        // TODO: Add warn logging for ambiguous passwords?
        return isUniqueKeyPasswordSet() ? getKeyPassword() : getKeystorePassword();
    }

    /**
     * Returns the password for the key being used -- this can be the <em>key</em> password if it is set, or the
     * <em>keystore</em> password if no key password is set. Either way, this value will be accepted by the keystore
     * when accessing the key.
     *
     * @return the key or keystore password
     */
    @Override
    public char[] getKeyOrKeystorePasswordAsChars() {
        String kp = getKeyOrKeystorePassword();
        return kp == null ? new char[0] : kp.toCharArray();
    }

    /**
     * Returns the {@link KeystoreType}.
     *
     * @return the keystore type
     */
    @Override
    public KeystoreType getKeystoreType() {
        return keystoreType;
    }

    /**
     * Returns the {@link KeystoreType} as a String. If no keystore type is set, returns {@code null}.
     *
     * @return the keystore type
     */
    @Override
    public String getKeystoreTypeAsString() {
        return keystoreType != null ? keystoreType.name() : null;
    }

    /**
     * Sets the keystore type.
     *
     * @param keystoreType the keystore type
     */
    @Override
    public void setKeystoreType(KeystoreType keystoreType) {
        this.keystoreType = keystoreType;
    }

    /**
     * Sets the keystore type. Throws an {@link IllegalArgumentException} if the {@link KeystoreType} cannot be parsed
     * from the input.
     *
     * @param keystoreType the keystore type
     */
    @Override
    public void setKeystoreType(String keystoreType) {
        this.keystoreType = keystoreType == null ? null : KeystoreType.valueOf(keystoreType.toUpperCase().trim());
    }

    /**
     * Returns the truststore path. Absolute paths are preferred.
     *
     * @return the truststore path
     */
    @Override
    public String getTruststorePath() {
        return truststorePath;
    }

    /**
     * Sets the truststore path. Absolute paths are preferred. Paths are trimmed for leading and trailing spaces.
     *
     * @param truststorePath the file path to the truststore resource
     */
    @Override
    public void setTruststorePath(String truststorePath) {
        if (truststorePath == null || truststorePath.trim().length() == 0) {
            this.truststorePath = null;
        } else {
            this.truststorePath = truststorePath.trim();
        }
    }

    /**
     * Returns the truststore password. If none is set, {@code null} is returned.
     *
     * @return the truststore password
     */
    @Override
    public String getTruststorePassword() {
        return truststorePassword;
    }

    /**
     * Returns the truststore password.
     *
     * @return the truststore password
     */
    @Override
    public char[] getTruststorePasswordAsChars() {
        return truststorePassword == null ? new char[0] : truststorePassword.toCharArray();
    }

    /**
     * Sets the truststore password. {@code null} passwords are not recommended, but accepted. Passwords are trimmed for
     * leading and trailing whitespace, and empty passwords are set as {@code null}.
     *
     * @param truststorePassword the truststore password
     */
    @Override
    public void setTruststorePassword(String truststorePassword) {
        // TODO: Add warn logging for null passwords?
        if (truststorePassword == null || truststorePassword.trim().length() == 0) {
            this.truststorePassword = null;
        } else {
            this.truststorePassword = truststorePassword.trim();
        }
    }

    /**
     * Sets the truststore password. {@code null} passwords are not recommended, but accepted. Passwords are trimmed for
     * leading and trailing whitespace, and empty passwords are set as {@code null}.
     *
     * @param truststorePassword the truststore password
     */
    @Override
    public void setTruststorePassword(char[] truststorePassword) {
        setTruststorePassword(String.valueOf(truststorePassword));
    }

    /**
     * Returns the {@link KeystoreType}.
     *
     * @return the truststore type
     */
    @Override
    public KeystoreType getTruststoreType() {
        return truststoreType;
    }

    /**
     * Returns the {@link KeystoreType} as a String.
     *
     * @return the truststore type
     */
    @Override
    public String getTruststoreTypeAsString() {
        return truststoreType != null ? truststoreType.name() : null;
    }

    /**
     * Sets the truststore type.
     *
     * @param truststoreType the truststore type
     */
    @Override
    public void setTruststoreType(KeystoreType truststoreType) {
        this.truststoreType = truststoreType;
    }

    /**
     * Sets the truststore type. Throws an {@link IllegalArgumentException} if the {@link KeystoreType} cannot be parsed
     * from the input.
     *
     * @param truststoreType the truststore type
     */
    @Override
    public void setTruststoreType(String truststoreType) {
        this.truststoreType = truststoreType == null ? null : KeystoreType.valueOf(truststoreType.toUpperCase().trim());
    }

    /**
     * Returns the {@link SslContextFactory.ClientAuth} setting. This only affects
     * incoming connections (i.e. when this SSL configuration is being used as a <em>server</em>).
     *
     * @return the client auth setting
     */
    @Override
    public SslContextFactory.ClientAuth getClientAuth() {
        return clientAuth;
    }

    /**
     * Returns the {@link SslContextFactory.ClientAuth} setting as a String. This only
     * affects incoming connections (i.e. when this SSL configuration is being used as a <em>server</em>).
     *
     * @return the client auth setting
     */
    @Override
    public String getClientAuthAsString() {
        return clientAuth == null ? null : clientAuth.name();
    }

    /**
     * Sets the {@link SslContextFactory.ClientAuth} setting. This only affects
     * incoming connections (i.e. when this SSL configuration is being used as a <em>server</em>).
     *
     * @param clientAuth
     * @@param clientAuth the client auth setting
     */
    @Override
    public void setClientAuth(SslContextFactory.ClientAuth clientAuth) {
        this.clientAuth = clientAuth;
    }

    /**
     * Sets the {@link SslContextFactory.ClientAuth} setting. This only affects
     * incoming connections (i.e. when this SSL configuration is being used as a <em>server</em>).
     *
     * @param clientAuth
     * @@param clientAuth the client auth setting
     */
    @Override
    public void setClientAuth(String clientAuth) {
        this.clientAuth = SslContextFactory.ClientAuth.valueOf(clientAuth == null ? "" : clientAuth.toUpperCase().trim());
    }

    /**
     * Returns the TLS/SSL protocol setting. This only affects incoming connections (i.e. when this SSL configuration is
     * being used as a <em>server</em>). Example values are "TLS", "TLSv1.2", etc.
     *
     * @return the protocol
     */
    @Override
    public String getProtocol() {
        return protocol;
    }

    /**
     * Sets the TLS/SSL protocol setting. This only affects incoming connections (i.e. when this SSL configuration is
     * being used as a <em>server</em>). Example values are "TLS", "TLSv1.2", etc.
     *
     * @@param protocol the protocol
     */
    @Override
    public void setProtocol(String protocol) {
        if (protocol == null || protocol.trim().length() == 0) {
            this.protocol = null;
        } else {
            this.protocol = protocol.trim();
        }
    }
}
