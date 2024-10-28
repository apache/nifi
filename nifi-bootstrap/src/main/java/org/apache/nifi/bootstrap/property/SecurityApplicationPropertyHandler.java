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
package org.apache.nifi.bootstrap.property;

import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.slf4j.Logger;

import javax.security.auth.x500.X500Principal;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard implementation for application security generates Key Pair and Certificate when not configured
 */
public class SecurityApplicationPropertyHandler implements ApplicationPropertyHandler {
    protected static final String ENTRY_ALIAS = "generated";

    protected static final X500Principal CERTIFICATE_ISSUER = new X500Principal("CN=localhost");

    private static final String DIGEST_ALGORITHM = "SHA-256";

    private static final String KEY_ALGORITHM = "RSA";

    private static final int KEY_SIZE = 4096;

    private static final String LOCALHOST = "localhost";

    private static final Duration CERTIFICATE_VALIDITY_PERIOD = Duration.ofDays(60);

    private static final int RANDOM_BYTE_LENGTH = 16;

    private static final String PROPERTY_SEPARATOR = "=";

    // Maximum address length based on RFC 1035 Section 2.3.4
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("^([\\w-.]{1,254}):?\\d{0,5}$");

    private static final int HOST_GROUP = 1;

    private static final Pattern HOST_PORT_GROUP_SEPARATOR = Pattern.compile("\\s*,\\s*");

    private final Logger logger;

    public SecurityApplicationPropertyHandler(final Logger logger) {
        this.logger = Objects.requireNonNull(logger, "Logger required");
    }

    @Override
    public void handleProperties(final Path applicationPropertiesLocation) {
        Objects.requireNonNull(applicationPropertiesLocation);

        final Properties applicationProperties = loadProperties(applicationPropertiesLocation);
        if (isCertificateGenerationRequired(applicationProperties)) {
            processApplicationProperties(applicationProperties);
            writePasswordProperties(applicationProperties, applicationPropertiesLocation);
        }
    }

    private void processApplicationProperties(final Properties applicationProperties) {
        final KeyPair keyPair = generateKeyPair();
        final Collection<String> subjectAlternativeNames = getSubjectAlternativeNames(applicationProperties);
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, CERTIFICATE_ISSUER, CERTIFICATE_VALIDITY_PERIOD)
                .setDnsSubjectAlternativeNames(subjectAlternativeNames)
                .build();
        final String certificateDigestEncoded = getDigest(certificate);

        logger.info("Generated Self-Signed Certificate Expiration: {}", LocalDate.now().plusDays(CERTIFICATE_VALIDITY_PERIOD.toDays()));
        logger.info("Generated Self-Signed Certificate SHA-256: {}", certificateDigestEncoded);

        final PrivateKey privateKey = keyPair.getPrivate();
        writeKeyStore(applicationProperties, certificate, privateKey);
        writeTrustStore(applicationProperties, certificate);
    }

    private void writeTrustStore(final Properties applicationProperties, final X509Certificate certificate) {
        final String storeType = applicationProperties.getProperty(SecurityProperty.TRUSTSTORE_TYPE.getName());
        final KeyStore trustStore = newKeyStore(storeType);
        try {
            trustStore.load(null, null);
            trustStore.setCertificateEntry(ENTRY_ALIAS, certificate);
        } catch (final GeneralSecurityException | IOException e) {
            throw new IllegalStateException("Trust Store creation failed", e);
        }

        final Path outputLocation = Paths.get(applicationProperties.getProperty(SecurityProperty.TRUSTSTORE.getName()));
        try (final OutputStream outputStream = Files.newOutputStream(outputLocation)) {
            final String truststorePasswd = generatePassword();
            trustStore.store(outputStream, truststorePasswd.toCharArray());

            applicationProperties.setProperty(SecurityProperty.TRUSTSTORE_PASSWD.getName(), truststorePasswd);
        } catch (final GeneralSecurityException | IOException e) {
            throw new IllegalStateException("Trust Store storage failed", e);
        }
    }

    private void writeKeyStore(final Properties applicationProperties, final X509Certificate certificate, final PrivateKey privateKey) {
        final String keystorePasswd = generatePassword();
        final char[] password = keystorePasswd.toCharArray();

        final String storeType = applicationProperties.getProperty(SecurityProperty.KEYSTORE_TYPE.getName());
        final KeyStore keyStore = newKeyStore(storeType);
        try {
            keyStore.load(null, null);
            final X509Certificate[] certificates = new X509Certificate[]{certificate};
            keyStore.setKeyEntry(ENTRY_ALIAS, privateKey, password, certificates);
        } catch (final GeneralSecurityException | IOException e) {
            throw new IllegalStateException("Key Store creation failed", e);
        }

        final Path outputLocation = Paths.get(applicationProperties.getProperty(SecurityProperty.KEYSTORE.getName()));
        try (final OutputStream outputStream = Files.newOutputStream(outputLocation)) {
            keyStore.store(outputStream, password);

            applicationProperties.setProperty(SecurityProperty.KEYSTORE_PASSWD.getName(), keystorePasswd);
            applicationProperties.setProperty(SecurityProperty.KEY_PASSWD.getName(), keystorePasswd);
        } catch (final GeneralSecurityException | IOException e) {
            throw new IllegalStateException("Key Store storage failed", e);
        }
    }

    private void writePasswordProperties(final Properties applicationProperties, final Path applicationPropertiesLocation) {
        final ByteArrayOutputStream propertiesOutputStream = new ByteArrayOutputStream();
        try (
                final BufferedReader reader = Files.newBufferedReader(applicationPropertiesLocation);
                final PrintWriter writer = new PrintWriter(propertiesOutputStream)
        ) {
            String line = reader.readLine();
            while (line != null) {
                if (line.startsWith(SecurityProperty.KEYSTORE_PASSWD.getName())) {
                    writeProperty(writer, SecurityProperty.KEYSTORE_PASSWD, applicationProperties);
                } else if (line.startsWith(SecurityProperty.KEY_PASSWD.getName())) {
                    writeProperty(writer, SecurityProperty.KEY_PASSWD, applicationProperties);
                } else if (line.startsWith(SecurityProperty.TRUSTSTORE_PASSWD.getName())) {
                    writeProperty(writer, SecurityProperty.TRUSTSTORE_PASSWD, applicationProperties);
                } else {
                    writer.println(line);
                }

                line = reader.readLine();
            }
        } catch (final IOException e) {
            throw new IllegalStateException("Read Application Properties failed", e);
        }

        final byte[] updatedProperties = propertiesOutputStream.toByteArray();
        try (final OutputStream outputStream = Files.newOutputStream(applicationPropertiesLocation)) {
            outputStream.write(updatedProperties);
        } catch (final IOException e) {
            throw new IllegalStateException("Write Application Properties failed", e);
        }
    }

    private void writeProperty(final PrintWriter writer, final SecurityProperty securityProperty, final Properties applicationProperties) {
        writer.print(securityProperty.getName());
        writer.print(PROPERTY_SEPARATOR);

        final String propertyValue = applicationProperties.getProperty(securityProperty.getName());
        writer.println(propertyValue);
    }

    private KeyStore newKeyStore(final String storeType) {
        try {
            return KeyStore.getInstance(storeType);
        } catch (final KeyStoreException e) {
            throw new IllegalStateException("Key Store Type [%s] instantiation failed".formatted(storeType), e);
        }
    }

    private boolean isCertificateGenerationRequired(final Properties applicationProperties) {
        final boolean required;

        final String keystoreLocation = applicationProperties.getProperty(SecurityProperty.KEYSTORE.getName());
        final String truststoreLocation = applicationProperties.getProperty(SecurityProperty.TRUSTSTORE.getName());

        if (isBlank(applicationProperties.getProperty(SecurityProperty.HTTPS_PORT.getName()))) {
            required = false;
        } else if (isBlank(keystoreLocation)) {
            required = false;
        } else if (isBlank(truststoreLocation)) {
            required = false;
        } else if (isBlank(applicationProperties.getProperty(SecurityProperty.KEYSTORE_PASSWD.getName()))
                    && isBlank(applicationProperties.getProperty(SecurityProperty.TRUSTSTORE_PASSWD.getName()))) {
            final Path keystorePath = Paths.get(keystoreLocation);
            final Path truststorePath = Paths.get(truststoreLocation);

            required = Files.notExists(keystorePath) && Files.notExists(truststorePath);
        } else {
            required = false;
        }

        return required;
    }

    private Collection<String> getSubjectAlternativeNames(final Properties applicationProperties) {
        try {
            final InetAddress localHost = InetAddress.getLocalHost();
            final String localHostName = localHost.getHostName();

            final List<String> subjectAlternativeNames = new ArrayList<>();
            subjectAlternativeNames.add(LOCALHOST);
            subjectAlternativeNames.add(localHostName);

            final String proxyHost = applicationProperties.getProperty(SecurityProperty.WEB_PROXY_HOST.getName());
            final Set<String> proxyHostNames = getHosts(proxyHost);
            subjectAlternativeNames.addAll(proxyHostNames);

            return subjectAlternativeNames;
        } catch (final UnknownHostException e) {
            return Collections.emptyList();
        }
    }

    private KeyPair generateKeyPair() {
        final KeyPairGenerator keyPairGenerator;
        try {
            keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("Key Pair Algorithm not supported [%s]".formatted(KEY_ALGORITHM), e);
        }

        keyPairGenerator.initialize(KEY_SIZE);
        return keyPairGenerator.generateKeyPair();
    }

    protected String generatePassword() {
        final SecureRandom secureRandom = new SecureRandom();
        final byte[] bytes = new byte[RANDOM_BYTE_LENGTH];
        secureRandom.nextBytes(bytes);
        return HexFormat.of().formatHex(bytes);
    }

    private static String getDigest(final X509Certificate certificate) {
        try {
            final MessageDigest messageDigest = MessageDigest.getInstance(DIGEST_ALGORITHM);
            final byte[] certificateEncoded = certificate.getEncoded();
            final byte[] digest = messageDigest.digest(certificateEncoded);
            return HexFormat.of().formatHex(digest).toUpperCase();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("Message Digest Algorithm not found", e);
        } catch (final CertificateEncodingException e) {
            throw new IllegalArgumentException("Certificate encoding processing failed", e);
        }
    }

    private Properties loadProperties(final Path applicationPropertiesLocation) {
        try (final InputStream inputStream = Files.newInputStream(applicationPropertiesLocation)) {
            final Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        } catch (final IOException e) {
            throw new IllegalStateException("Reading Application Properties failed [%s]".formatted(applicationPropertiesLocation), e);
        }
    }

    private Set<String> getHosts(final String property) {
        final Set<String> hosts = new HashSet<>();

        if (property != null) {
            final String[] hostPortGroups = HOST_PORT_GROUP_SEPARATOR.split(property);
            for (final String hostPortGroup : hostPortGroups) {
                final Matcher hostPortMatcher = HOST_PORT_PATTERN.matcher(hostPortGroup);
                if (hostPortMatcher.matches()) {
                    final String host = hostPortMatcher.group(HOST_GROUP);
                    hosts.add(host);
                } else {
                    logger.warn("Invalid host [{}] configured for [{}] in nifi.properties", hostPortGroup, SecurityProperty.WEB_PROXY_HOST.getName());
                }
            }
        }

        return hosts;
    }

    private boolean isBlank(final String propertyValue) {
        return propertyValue == null || propertyValue.isBlank();
    }

    protected enum SecurityProperty {
        HTTPS_PORT("nifi.web.https.port"),

        WEB_PROXY_HOST("nifi.web.proxy.host"),

        KEYSTORE("nifi.security.keystore"),

        KEYSTORE_TYPE("nifi.security.keystoreType"),

        KEYSTORE_PASSWD("nifi.security.keystorePasswd"),

        KEY_PASSWD("nifi.security.keyPasswd"),

        TRUSTSTORE("nifi.security.truststore"),

        TRUSTSTORE_TYPE("nifi.security.truststoreType"),

        TRUSTSTORE_PASSWD("nifi.security.truststorePasswd");

        private final String name;

        SecurityProperty(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
