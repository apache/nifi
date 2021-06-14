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
package org.apache.nifi.bootstrap.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.util.IPAddress;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class SecureNiFiConfigUtil {

    private static final int CERT_DURATION_DAYS = 60;
    private static final String LOCALHOST_NAME = "localhost";
    private static final String PROPERTY_VALUE_PATTERN = "%s=%s";

    private SecureNiFiConfigUtil() {

    }

    private static boolean fileExists(String filename) {
        return StringUtils.isNotEmpty(filename) && Paths.get(filename).toFile().exists();
    }

    /**
     * Returns true only if nifi.web.https.port is set, nifi.security.keystore and nifi.security.truststore
     * are both set, and both nifi.security.keystorePasswd and nifi.security.truststorePassword are NOT set.
     *
     * This would indicate that the user intends to auto-generate a keystore and truststore, rather than
     * using their existing kestore and truststore.
     * @param nifiProperties The nifi properties
     * @return
     */
    private static boolean isHttpsSecurityConfiguredWithEmptyPasswords(final Properties nifiProperties) {
        if (StringUtils.isEmpty(nifiProperties.getProperty(NiFiProperties.WEB_HTTPS_PORT, StringUtils.EMPTY))) {
            return false;
        }

        String keystorePath = nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE, StringUtils.EMPTY);
        String truststorePath = nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE, StringUtils.EMPTY);
        if (StringUtils.isEmpty(keystorePath) || StringUtils.isEmpty(truststorePath)) {
            return false;
        }

        String keystorePassword = nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD, StringUtils.EMPTY);
        String truststorePassword = nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, StringUtils.EMPTY);
        if (StringUtils.isNotEmpty(keystorePassword) || StringUtils.isNotEmpty(truststorePassword)) {
            return false;
        }

        return true;
    }

    /**
     * If HTTPS is enabled (nifi.web.https.port is set), but the keystore file specified in nifi.security.keystore
     * does not exist, this will generate a key pair and self-signed certificate, generate the associated keystore
     * and truststore and write them to disk under the configured filepaths, generate a secure random keystore password
     * and truststore password, and write these to the nifi.properties file.
     * @param nifiPropertiesFilename The filename of the nifi.properties file
     * @param cmdLogger The bootstrap logger
     * @throws IOException can be thrown when writing keystores to disk
     * @throws RuntimeException indicates a security exception while generating keystores
     */
    public static void configureSecureNiFiProperties(final String nifiPropertiesFilename, final Logger cmdLogger) throws IOException, RuntimeException {
        final File propertiesFile = new File(nifiPropertiesFilename);
        final Properties nifiProperties = loadProperties(propertiesFile);

        if (!isHttpsSecurityConfiguredWithEmptyPasswords(nifiProperties)) {
            cmdLogger.debug("Skipping Apache Nifi certificate generation.");
            return;
        }

        String keystorePath = nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE, StringUtils.EMPTY);
        String truststorePath = nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE, StringUtils.EMPTY);
        boolean keystoreExists = fileExists(keystorePath);
        boolean truststoreExists = fileExists(truststorePath);

        if (!keystoreExists && !truststoreExists) {
            TlsConfiguration tlsConfiguration;
            cmdLogger.info("Generating Self-Signed Certificate: Expires on {}", LocalDate.now().plus(CERT_DURATION_DAYS, ChronoUnit.DAYS));
            try {
                String[] subjectAlternativeNames = getSubjectAlternativeNames(nifiProperties, cmdLogger);
                tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore(StandardTlsConfiguration
                        .fromNiFiProperties(nifiProperties), CERT_DURATION_DAYS, subjectAlternativeNames);
                final KeyStore keyStore = KeyStoreUtils.loadKeyStore(tlsConfiguration.getKeystorePath(),
                        tlsConfiguration.getKeystorePassword().toCharArray(), tlsConfiguration.getKeystoreType().getType());
                final Enumeration<String> aliases = keyStore.aliases();
                while (aliases.hasMoreElements()) {
                    final String alias = aliases.nextElement();
                    final Certificate certificate = keyStore.getCertificate(alias);
                    if (certificate != null) {
                        final String sha256 = DigestUtils.sha256Hex(certificate.getEncoded());
                        cmdLogger.info("Generated Self-Signed Certificate SHA-256: {}", sha256.toUpperCase(Locale.ROOT));
                    }
                }
            } catch (GeneralSecurityException e) {
                throw new RuntimeException(e);
            }

            // Move over the new stores from temp dir
            Files.move(Paths.get(tlsConfiguration.getKeystorePath()), Paths.get(keystorePath),
                    StandardCopyOption.REPLACE_EXISTING);
            Files.move(Paths.get(tlsConfiguration.getTruststorePath()), Paths.get(truststorePath),
                    StandardCopyOption.REPLACE_EXISTING);

            updateProperties(propertiesFile, tlsConfiguration);

            cmdLogger.debug("Generated Keystore [{}] Truststore [{}]", keystorePath, truststorePath);
        } else if (!keystoreExists && truststoreExists) {
            cmdLogger.warn("Truststore file {} already exists.  Apache NiFi will not generate keystore and truststore separately.",
                    truststorePath);
        } else if (keystoreExists && !truststoreExists) {
            cmdLogger.warn("Keystore file {} already exists.  Apache NiFi will not generate keystore and truststore separately.",
                    keystorePath);
        }
    }

    /**
     * Attempts to add some reasonable guesses at desired SAN values that can be added to the generated
     * certificate.
     * @param nifiProperties The nifi.properties
     * @return A Pair with IP SANs on the left and DNS SANs on the right
     */
    private static String[] getSubjectAlternativeNames(Properties nifiProperties, Logger cmdLogger) {
        Set<String> dnsSubjectAlternativeNames = new HashSet<>();

        try {
            dnsSubjectAlternativeNames.add(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            cmdLogger.debug("Could not add localhost hostname as certificate SAN", e);
        }
        addSubjectAlternativeName(nifiProperties, NiFiProperties.REMOTE_INPUT_HOST, dnsSubjectAlternativeNames);
        addSubjectAlternativeName(nifiProperties, NiFiProperties.WEB_HTTPS_HOST, dnsSubjectAlternativeNames);
        addSubjectAlternativeName(nifiProperties, NiFiProperties.WEB_PROXY_HOST, dnsSubjectAlternativeNames);
        addSubjectAlternativeName(nifiProperties, NiFiProperties.LOAD_BALANCE_HOST, dnsSubjectAlternativeNames);

        // Not necessary to add as a SAN
        dnsSubjectAlternativeNames.remove(LOCALHOST_NAME);

        return dnsSubjectAlternativeNames.toArray(new String[dnsSubjectAlternativeNames.size()]);
    }

    private static void addSubjectAlternativeName(Properties nifiProperties, String propertyName,
                                                  Set<String> dnsSubjectAlternativeNames) {
        String hostValue = nifiProperties.getProperty(propertyName, StringUtils.EMPTY);
        if (!hostValue.isEmpty()) {
            if (!IPAddress.isValid(hostValue)) {
                dnsSubjectAlternativeNames.add(hostValue);
            }
        }
    }

    private static String getPropertyLine(String name, String value) {
        return String.format(PROPERTY_VALUE_PATTERN, name, value);
    }

    private static void updateProperties(final File propertiesFile, final TlsConfiguration tlsConfiguration) throws IOException {
        final Path propertiesFilePath = propertiesFile.toPath();
        final List<String> lines = Files.readAllLines(propertiesFilePath);
        final List<String> updatedLines = lines.stream().map(line -> {
            if (line.startsWith(NiFiProperties.SECURITY_KEYSTORE_PASSWD)) {
                return getPropertyLine(NiFiProperties.SECURITY_KEYSTORE_PASSWD, tlsConfiguration.getKeystorePassword());
            } else if (line.startsWith(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD)) {
                return getPropertyLine(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, tlsConfiguration.getTruststorePassword());
            } else if (line.startsWith(NiFiProperties.SECURITY_KEY_PASSWD)) {
                return getPropertyLine(NiFiProperties.SECURITY_KEY_PASSWD, tlsConfiguration.getKeystorePassword());
            } else if (line.startsWith(NiFiProperties.SECURITY_KEYSTORE_TYPE)) {
                return getPropertyLine(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
            } else if (line.startsWith(NiFiProperties.SECURITY_TRUSTSTORE_TYPE)) {
                return getPropertyLine(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
            } else {
                return line;
            }
        }).collect(Collectors.toList());
        Files.write(propertiesFilePath, updatedLines);
    }

    private static Properties loadProperties(final File propertiesFile) {
        final Properties properties = new Properties();
        try (final FileReader reader = new FileReader(propertiesFile)) {
            properties.load(reader);
        } catch (final IOException e) {
            final String message = String.format("Failed to read NiFi Properties [%s]", propertiesFile);
            throw new UncheckedIOException(message, e);
        }
        return properties;
    }
}
