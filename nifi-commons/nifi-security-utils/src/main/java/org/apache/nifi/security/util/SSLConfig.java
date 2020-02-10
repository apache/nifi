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

/**
 * This class provides a container interface for passing common TLS/SSL configuration values around between extensions
 * (components) and the core framework and security utilities which generate the internal JCA/JCE classes to create the
 * secure connections.
 */
public interface SSLConfig {

    // Keystore properties

    /**
     * Returns the keystore alias to use.
     *
     * @return the alias
     */
    String getAlias();

    /**
     * Sets the keystore alias to use. If none is provided, the first suitable alias encountered will be used. Aliases
     * are trimmed for leading and trailing spaces.
     *
     * @param alias the alias identifier
     */
    void setAlias(String alias);

    /**
     * Returns the keystore path. Absolute paths are preferred.
     *
     * @return the keystore path
     */
    String getKeystorePath();

    /**
     * Sets the keystore path. Absolute paths are preferred. Paths are trimmed for leading and trailing spaces.
     *
     * @param keystorePath the file path to the keystore resource
     */
    void setKeystorePath(String keystorePath);

    /**
     * Returns the keystore password.
     *
     * @return the keystore password
     */
    String getKeystorePassword();

    /**
     * Returns the keystore password.
     *
     * @return the keystore password
     */
    char[] getKeystorePasswordAsChars();

    /**
     * Sets the keystore password. Leading and trailing spaces are trimmed.
     *
     * @param keystorePassword the keystore password
     */
    void setKeystorePassword(String keystorePassword);

    /**
     * Sets the keystore password. Leading and trailing spaces are trimmed.
     *
     * @param keystorePassword the keystore password
     */
    void setKeystorePassword(char[] keystorePassword);

    /**
     * Returns the key password. If none is set, {@code null} is returned.
     *
     * See also {@link #getKeyOrKeystorePassword()} and {@link #isUniqueKeyPasswordSet()}.
     *
     * @return the key password
     */
    String getKeyPassword();

    /**
     * Returns the key password.
     *
     * @return the key password
     */
    char[] getKeyPasswordAsChars();

    /**
     * Sets the key password. Accepts {@code null} input. Leading and trailing spaces are trimmed.
     *
     * @param keyPassword the key password
     */
    void setKeyPassword(String keyPassword);

    /**
     * Sets the key password. Accepts {@code null} input. Leading and trailing spaces are trimmed.
     *
     * @param keyPassword the key password
     */
    void setKeyPassword(char[] keyPassword);

    /**
     * Returns {@code true} if a password value is set for the <em>key</em> which is different from the <em>keystore</em>
     * password. If the key password is {@code null} or identical to the keystore password, returns {@code false}.
     *
     * @return true if a unique key password is set
     */
    boolean isUniqueKeyPasswordSet();

    /**
     * Returns the password for the key being used -- this can be the <em>key</em> password if it is set, or the
     * <em>keystore</em> password if no key password is set. Either way, this value will be accepted by the keystore
     * when accessing the key.
     *
     * @return the key or keystore password
     */
    String getKeyOrKeystorePassword();

    /**
     * Returns the password for the key being used -- this can be the <em>key</em> password if it is set, or the
     * <em>keystore</em> password if no key password is set. Either way, this value will be accepted by the keystore
     * when accessing the key.
     *
     * @return the key or keystore password
     */
    char[] getKeyOrKeystorePasswordAsChars();

    /**
     * Returns the {@link KeystoreType}.
     *
     * @return the keystore type
     */
    KeystoreType getKeystoreType();

    /**
     * Returns the {@link KeystoreType} as a String. If no keystore type is set, returns {@code null}.
     *
     * @return the keystore type
     */
    String getKeystoreTypeAsString();

    /**
     * Sets the keystore type.
     *
     * @param keystoreType the keystore type
     */
    void setKeystoreType(KeystoreType keystoreType);

    /**
     * Sets the keystore type. Throws an {@link IllegalArgumentException} if the {@link KeystoreType} cannot be parsed
     * from the input.
     *
     * @param keystoreType the keystore type
     */
    void setKeystoreType(String keystoreType);

    // Truststore properties

    /**
     * Returns the truststore path. Absolute paths are preferred.
     *
     * @return the truststore path
     */
    String getTruststorePath();

    /**
     * Sets the truststore path. Absolute paths are preferred.
     *
     * @param truststorePath the file path to the truststore resource
     */
    void setTruststorePath(String truststorePath);

    /**
     * Returns the truststore password. If none is set, {@code null} is returned.
     *
     * @return the truststore password
     */
    String getTruststorePassword();

    /**
     * Returns the truststore password.
     *
     * @return the truststore password
     */
    char[] getTruststorePasswordAsChars();

    /**
     * Sets the truststore password. {@code null} passwords are not recommended, but accepted. Passwords are trimmed for
     * leading and trailing whitespace, and empty passwords are set as {@code null}.
     *
     * @param truststorePassword the truststore password
     */
    void setTruststorePassword(String truststorePassword);

    /**
     * Sets the truststore password. {@code null} passwords are not recommended, but accepted. Passwords are trimmed for
     * leading and trailing whitespace, and empty passwords are set as {@code null}.
     *
     * @param truststorePassword the truststore password
     */
    void setTruststorePassword(char[] truststorePassword);

    /**
     * Returns the {@link KeystoreType}.
     *
     * @return the truststore type
     */
    KeystoreType getTruststoreType();

    /**
     * Returns the {@link KeystoreType} as a String.
     *
     * @return the truststore type
     */
    String getTruststoreTypeAsString();

    /**
     * Sets the truststore type.
     *
     * @param truststoreType the truststore type
     */
    void setTruststoreType(KeystoreType truststoreType);

    /**
     * Sets the truststore type. Throws an {@link IllegalArgumentException} if the {@link KeystoreType} cannot be parsed
     * from the input.
     *
     * @param truststoreType the truststore type
     */
    void setTruststoreType(String truststoreType);

    // Other properties

    /**
     * Returns the {@link org.apache.nifi.security.util.SslContextFactory.ClientAuth} setting. This only affects
     * incoming connections (i.e. when this SSL configuration is being used as a <em>server</em>).
     *
     * @return the client auth setting
     */
    SslContextFactory.ClientAuth getClientAuth();

    /**
     * Returns the {@link org.apache.nifi.security.util.SslContextFactory.ClientAuth} setting as a String. This only
     * affects incoming connections (i.e. when this SSL configuration is being used as a <em>server</em>).
     *
     * @return the client auth setting
     */
    String getClientAuthAsString();

    /**
     * Sets the {@link org.apache.nifi.security.util.SslContextFactory.ClientAuth} setting. This only affects
     * incoming connections (i.e. when this SSL configuration is being used as a <em>server</em>).
     *
     * @@param clientAuth the client auth setting
     */
    void setClientAuth(SslContextFactory.ClientAuth clientAuth);

    /**
     * Sets the {@link org.apache.nifi.security.util.SslContextFactory.ClientAuth} setting. This only affects
     * incoming connections (i.e. when this SSL configuration is being used as a <em>server</em>).
     *
     * @@param clientAuth the client auth setting
     */
    void setClientAuth(String clientAuth);

    /**
     * Returns the TLS/SSL protocol setting. This only affects incoming connections (i.e. when this SSL configuration is
     * being used as a <em>server</em>). Example values are "TLS", "TLSv1.2", etc.
     *
     * @return the protocol
     */
    String getProtocol();

    /**
     * Sets the TLS/SSL protocol setting. This only affects incoming connections (i.e. when this SSL configuration is
     * being used as a <em>server</em>). Example values are "TLS", "TLSv1.2", etc.
     *
     * @@param protocol the protocol
     */
    void setProtocol(String protocol);
}

