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

import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.configuration.KeyStoreConfiguration;

import javax.security.auth.x500.X500Principal;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * KeyStore Factory for creating temporary files primarily used for testing
 */
public class TemporaryKeyStoreBuilder {
    private static final String KEY_PAIR_ALGORITHM = "RSA";

    private static final int KEY_SIZE = 2048;

    private static final int RANDOM_BYTES_LENGTH = 16;

    private static final Base64.Encoder ENCODER = Base64.getEncoder().withoutPadding();

    private static final String SIGNING_ALGORITHM = "SHA256withRSA";

    private static final String DISTINGUISHED_NAME_FORMAT = "CN=%s";

    private static final int CERTIFICATE_VALID_DAYS = 1;

    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.PKCS12;

    private static final String KEY_STORE_EXTENSION = ".p12";

    private static final String KEY_STORE_PREFIX = "TemporaryKeyStore-";

    private static final String DEFAULT_HOSTNAME = "localhost";

    private String hostname = DEFAULT_HOSTNAME;

    private String trustStorePassword = generateSecureRandomPassword();

    private String trustStoreType = KEYSTORE_TYPE.getType();

    /**
     * Set Hostname used for Certificate Common Name and DNS Subject Alternative Names
     *
     * @param hostname Hostname is required
     * @return Builder
     */
    public TemporaryKeyStoreBuilder hostname(final String hostname) {
        this.hostname = Objects.requireNonNull(hostname, "Hostname required");
        return this;
    }

    /**
     * Set Trust Store Password used for protected generated Trust Store file
     *
     * @param trustStorePassword Trust Store Password is required
     * @return Builder
     */
    public TemporaryKeyStoreBuilder trustStorePassword(final String trustStorePassword) {
        this.trustStorePassword = Objects.requireNonNull(trustStorePassword, "TrustStore Password required");
        return this;
    }

    /**
     * Set Trust Store Type used for storing Trust Store files
     *
     * @param trustStoreType Trust Store type must be a supported value for KeyStore.getInstance()
     * @return Builder
     */
    public TemporaryKeyStoreBuilder trustStoreType(final String trustStoreType) {
        this.trustStoreType = Objects.requireNonNull(trustStoreType, "TrustStore Type required");
        return this;
    }

    /**
     * Build Temporary KeyStore and TrustStore with configured values and set files with File.deleteOnExit()
     *
     * @return TLS Configuration with KeyStore and TrustStore properties
     */
    public TlsConfiguration build() {
        final KeyPair keyPair = generateKeyPair();
        final X509Certificate certificate = generateCertificate(hostname, keyPair);

        final KeyStoreConfiguration keyStoreConfiguration = setKeyStore(keyPair.getPrivate(), certificate);
        final KeyStoreConfiguration trustStoreConfiguration = setTrustStore(certificate);

        return new StandardTlsConfiguration(
                keyStoreConfiguration.getLocation(),
                keyStoreConfiguration.getPassword(),
                keyStoreConfiguration.getPassword(),
                keyStoreConfiguration.getKeyStoreType(),
                trustStoreConfiguration.getLocation(),
                trustStoreConfiguration.getPassword(),
                trustStoreConfiguration.getKeyStoreType(),
                TlsPlatform.getLatestProtocol()
        );
    }

    private KeyStoreConfiguration setKeyStore(final PrivateKey privateKey, final X509Certificate certificate) {
        final KeyStore keyStore = getNewKeyStore(KEYSTORE_TYPE.getType());

        final String password = generateSecureRandomPassword();
        final String alias = UUID.randomUUID().toString();
        try {
            keyStore.setKeyEntry(alias, privateKey, password.toCharArray(), new Certificate[]{certificate});
        } catch (final KeyStoreException e) {
            throw new RuntimeException("Set Key Entry Failed", e);
        }

        final File keyStoreFile = storeKeyStore(keyStore, password.toCharArray());
        return new KeyStoreConfiguration(keyStoreFile.getAbsolutePath(), password, KEYSTORE_TYPE.getType());
    }

    private KeyStoreConfiguration setTrustStore(final X509Certificate certificate) {
        final KeyStore keyStore = getNewKeyStore(trustStoreType);

        final String alias = UUID.randomUUID().toString();
        try {
            keyStore.setCertificateEntry(alias, certificate);
        } catch (final KeyStoreException e) {
            throw new RuntimeException("Set Certificate Entry Failed", e);
        }

        final File trustStoreFile = storeKeyStore(keyStore, trustStorePassword.toCharArray());
        return new KeyStoreConfiguration(trustStoreFile.getAbsolutePath(), trustStorePassword, trustStoreType);
    }

    private File storeKeyStore(final KeyStore keyStore, final char[] password) {
        try {
            final File keyStoreFile = File.createTempFile(KEY_STORE_PREFIX, KEY_STORE_EXTENSION);
            keyStoreFile.deleteOnExit();
            try (final OutputStream outputStream = new FileOutputStream(keyStoreFile)) {
                keyStore.store(outputStream, password);
            }
            return keyStoreFile;
        } catch (final IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            throw new RuntimeException("Store KeyStore Failed", e);
        }
    }

    private KeyStore getNewKeyStore(final String newKeyStoreType) {
        try {
            final KeyStore keyStore = KeyStoreUtils.getKeyStore(newKeyStoreType);
            keyStore.load(null);
            return keyStore;
        } catch (final KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new RuntimeException(String.format("Create KeyStore [%s] Failed", KEYSTORE_TYPE), e);
        }
    }

    private X509Certificate generateCertificate(final String hostname, final KeyPair keyPair) {
        final X500Principal distinguishedName = new X500Principal(String.format(DISTINGUISHED_NAME_FORMAT, hostname));
        final List<String> dnsNames = Collections.singletonList(hostname);
        return new StandardCertificateBuilder(keyPair, distinguishedName, Duration.ofDays(CERTIFICATE_VALID_DAYS))
                .setDnsSubjectAlternativeNames(dnsNames)
                .build();
    }

    private KeyPair generateKeyPair() {
        try {
            final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_PAIR_ALGORITHM);
            keyPairGenerator.initialize(KEY_SIZE);
            return keyPairGenerator.generateKeyPair();
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(String.format("[%s] Algorithm not found", KEY_PAIR_ALGORITHM), e);
        }
    }

    private String generateSecureRandomPassword() {
        final SecureRandom secureRandom = new SecureRandom();
        final byte[] randomBytes = new byte[RANDOM_BYTES_LENGTH];
        secureRandom.nextBytes(randomBytes);
        return ENCODER.encodeToString(randomBytes);
    }
}
