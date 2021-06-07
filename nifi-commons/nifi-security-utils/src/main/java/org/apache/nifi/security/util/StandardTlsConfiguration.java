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

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;


/**
 * This class serves as a concrete immutable domain object (acting as an internal DTO)
 * for the various keystore and truststore configuration settings necessary for
 * building {@link javax.net.ssl.SSLContext}s.
 */
public class StandardTlsConfiguration implements TlsConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(StandardTlsConfiguration.class);

    private static final String TLS_PROTOCOL_VERSION = TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion();
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
     * Default constructor present for testing and completeness.
     */
    public StandardTlsConfiguration() {
        this(null, null, null, "", null, null, "", null);
    }

    /**
     * Instantiates a container object with the given configuration values.
     *
     * @param keystorePath       the keystore path
     * @param keystorePassword   the keystore password
     * @param keystoreType       the keystore type
     * @param truststorePath     the truststore path
     * @param truststorePassword the truststore password
     * @param truststoreType     the truststore type
     */
    public StandardTlsConfiguration(String keystorePath, String keystorePassword, KeystoreType keystoreType, String truststorePath, String truststorePassword, KeystoreType truststoreType) {
        this(keystorePath, keystorePassword, keystorePassword, keystoreType, truststorePath, truststorePassword, truststoreType, TLS_PROTOCOL_VERSION);
    }

    /**
     * Instantiates a container object with the given configuration values.
     *
     * @param keystorePath       the keystore path
     * @param keystorePassword   the keystore password
     * @param keyPassword        the key password
     * @param keystoreType       the keystore type
     * @param truststorePath     the truststore path
     * @param truststorePassword the truststore password
     * @param truststoreType     the truststore type
     */
    public StandardTlsConfiguration(String keystorePath, String keystorePassword, String keyPassword,
                            KeystoreType keystoreType, String truststorePath, String truststorePassword, KeystoreType truststoreType) {
        this(keystorePath, keystorePassword, keyPassword, keystoreType, truststorePath, truststorePassword, truststoreType, TLS_PROTOCOL_VERSION);
    }

    /**
     * Instantiates a container object with the given configuration values.
     *
     * @param keystorePath       the keystore path
     * @param keystorePassword   the keystore password
     * @param keyPassword        the key password
     * @param keystoreType       the keystore type as a String
     * @param truststorePath     the truststore path
     * @param truststorePassword the truststore password
     * @param truststoreType     the truststore type as a String
     */
    public StandardTlsConfiguration(String keystorePath, String keystorePassword, String keyPassword,
                            String keystoreType, String truststorePath, String truststorePassword, String truststoreType) {
        this(keystorePath, keystorePassword, keyPassword,
                (KeystoreType.isValidKeystoreType(keystoreType) ? KeystoreType.valueOf(keystoreType.toUpperCase()) : null),
                truststorePath, truststorePassword,
                (KeystoreType.isValidKeystoreType(truststoreType) ? KeystoreType.valueOf(truststoreType.toUpperCase()) : null),
                TLS_PROTOCOL_VERSION);
    }

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

    /**
     * Instantiates a container object with a deep copy of the given configuration values.
     *
     * @param other the configuration to copy
     */
    public StandardTlsConfiguration(TlsConfiguration other) {
        this.keystorePath = other.getKeystorePath();
        this.keystorePassword = other.getKeystorePassword();
        this.keyPassword = other.getKeyPassword();
        this.keystoreType = other.getKeystoreType();
        this.truststorePath = other.getTruststorePath();
        this.truststorePassword = other.getTruststorePassword();
        this.truststoreType = other.getTruststoreType();
        this.protocol = other.getProtocol();
    }

    // Static factory method from NiFiProperties

    /**
     * Returns a {@link org.apache.nifi.security.util.TlsConfiguration} instantiated from the relevant NiFi properties.
     *
     * @param niFiProperties the NiFi properties
     * @return a populated TlsConfiguration container object
     */
    public static TlsConfiguration fromNiFiProperties(final NiFiProperties niFiProperties) {
        final Properties properties = new Properties();
        niFiProperties.getPropertyKeys().forEach(key -> properties.setProperty(key, niFiProperties.getProperty(key)));
        return fromNiFiProperties(properties);
    }

    /**
     * Returns a {@link org.apache.nifi.security.util.TlsConfiguration} instantiated from the relevant NiFi properties.
     *
     * @param niFiProperties the NiFi properties, as a simple java.util.Properties object
     * @return a populated TlsConfiguration container object
     */
    public static TlsConfiguration fromNiFiProperties(final Properties niFiProperties) {
        Objects.requireNonNull("The NiFi properties cannot be null");

        String keystorePath = niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE);
        String keystorePassword = niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD);
        String keyPassword = niFiProperties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD);
        String keystoreType = niFiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE);
        String truststorePath = niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE);
        String truststorePassword = niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD);
        String truststoreType = niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE);
        String protocol = TLS_PROTOCOL_VERSION;

        final StandardTlsConfiguration tlsConfiguration = new StandardTlsConfiguration(keystorePath, keystorePassword, keyPassword,
                keystoreType, truststorePath, truststorePassword,
                truststoreType, protocol);

        return tlsConfiguration;
    }

    /**
     * Returns a {@link org.apache.nifi.security.util.TlsConfiguration} instantiated
     * from the relevant {@link NiFiProperties} properties for the truststore
     * <em>only</em>. No keystore properties are read or used.
     *
     * @param niFiProperties the NiFi properties
     * @return a populated TlsConfiguration container object
     */
    public static StandardTlsConfiguration fromNiFiPropertiesTruststoreOnly(NiFiProperties niFiProperties) {
        if (niFiProperties == null) {
            throw new IllegalArgumentException("The NiFi properties cannot be null");
        }

        String truststorePath = niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE);
        String truststorePassword = niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD);
        String truststoreType = niFiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE);
        String protocol = TLS_PROTOCOL_VERSION;

        final StandardTlsConfiguration tlsConfiguration = new StandardTlsConfiguration(null, null, null, null, truststorePath, truststorePassword,
                truststoreType, protocol);
        if (logger.isDebugEnabled()) {
            logger.debug("Instantiating TlsConfiguration from NiFi properties: null x4, {}, {}, {}, {}",
                    truststorePath, tlsConfiguration.getTruststorePasswordForLogging(), truststoreType, protocol);
        }

        return tlsConfiguration;
    }

    // /**
    //  * Returns {@code true} if the provided TlsConfiguration is {@code null} or <em>empty</em>
    //  * (i.e. neither any of the keystore nor truststore properties are populated).
    //  *
    //  * @param tlsConfiguration the container object to check
    //  * @return true if this container is empty or null
    //  */
    // public static boolean isEmpty(org.apache.nifi.security.util.TlsConfiguration tlsConfiguration) {
    //     return tlsConfiguration == null || !(tlsConfiguration.isAnyKeystorePopulated() || tlsConfiguration.isAnyTruststorePopulated());
    // }

    // Getters & setters

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
        return StringUtils.isNotBlank(keyPassword) ? keyPassword : keystorePassword;
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
        return isStorePopulated(keystorePath, keystorePassword, keystoreType, "keystore");
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
        boolean simpleCheck = isStoreValid(keystorePath, keystorePassword, keystoreType, "keystore");
        if (simpleCheck) {
            return true;
        } else if (StringUtils.isNotBlank(keyPassword) && !keystorePassword.equals(keyPassword)) {
            logger.debug("Simple keystore validity check failed; trying with separate key password");
            try {
                return isKeystorePopulated()
                        && KeyStoreUtils.isKeyPasswordCorrect(new File(keystorePath).toURI().toURL(), keystoreType, keystorePassword.toCharArray(),
                        getFunctionalKeyPassword().toCharArray());
            } catch (MalformedURLException e) {
                logger.error("Encountered an error validating the keystore: " + e.getLocalizedMessage());
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * Returns {@code true} if the necessary properties are populated to instantiate a <strong>truststore</strong>. This does <em>not</em> validate the values (see {@link #isTruststoreValid()}).
     *
     * @return true if the path, password, and type are present
     */
    @Override
    public boolean isTruststorePopulated() {
        return isStorePopulated(truststorePath, truststorePassword, truststoreType, "truststore");
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
        return isStoreValid(truststorePath, truststorePassword, truststoreType, "truststore");
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
            enabledProtocols.addAll(Arrays.asList(TlsConfiguration.getCurrentSupportedTlsProtocolVersions()));
        } else if (SSL_PROTOCOL.equals(configuredProtocol)) {
            enabledProtocols.addAll(Arrays.asList(LEGACY_TLS_PROTOCOL_VERSIONS));
            enabledProtocols.addAll(Arrays.asList(TlsConfiguration.getCurrentSupportedTlsProtocolVersions()));
        } else if (configuredProtocol != null) {
            enabledProtocols.add(configuredProtocol);
        }

        return enabledProtocols.toArray(new String[enabledProtocols.size()]);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[TlsConfiguration]");
        sb.append("keystorePath=").append(keystorePath);
        sb.append(",keystorePassword=").append(getKeystorePasswordForLogging());
        sb.append(",keyPassword=").append(getKeyPasswordForLogging());
        sb.append(",keystoreType=").append(keystoreType);
        sb.append(",truststorePath=").append(truststorePath);
        sb.append(",truststorePassword=").append(getTruststorePasswordForLogging());
        sb.append(",truststoreType=").append(truststoreType);
        sb.append(",protocol=").append(protocol);
        return sb.toString();
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
        return StringUtils.isNotBlank(password) ? MASKED_PASSWORD_LOG : NULL_LOG;
    }

    private boolean isAnyPopulated(String path, String password, KeystoreType type) {
        return StringUtils.isNotBlank(path) || StringUtils.isNotBlank(password) || type != null;
    }

    private boolean isStorePopulated(String path, String password, KeystoreType type, String label) {
        boolean isPopulated;
        String passwordForLogging;

        // Legacy truststores can be populated without a password; only check the path and type
        isPopulated = StringUtils.isNotBlank(path) && type != null;
        if ("truststore".equalsIgnoreCase(label)) {
            passwordForLogging = getTruststorePasswordForLogging();
        } else {
            // Keystores require a password
            isPopulated = isPopulated && StringUtils.isNotBlank(password);
            passwordForLogging = getKeystorePasswordForLogging();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("TLS config {} is {}: {}, {}, {}", label, isPopulated ? "populated" : "not populated", path, passwordForLogging, type);
        }
        return isPopulated;
    }

    private boolean isStoreValid(String path, String password, KeystoreType type, String label) {
        try {
            return isStorePopulated(path, password, type, label) && KeyStoreUtils.isStoreValid(new File(path).toURI().toURL(), type, password.toCharArray());
        } catch (MalformedURLException e) {
            logger.error("Encountered an error validating the " + label + ": " + e.getLocalizedMessage());
            return false;
        }
    }
}
