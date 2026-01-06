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
package org.apache.nifi.tests.system;

import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.security.auth.x500.X500Principal;

/**
 * System Key Store Provider generates a Key Pair and Certificate for KeyStore and TrustStore files
 */
public class NiFiSystemKeyStoreProvider {
    private static final String HOSTNAME = "localhost";

    private static final List<String> DNS_NAMES = Collections.singletonList(HOSTNAME);

    private static final X500Principal DISTINGUISHED_NAME = new X500Principal(String.format("CN=%s", HOSTNAME));

    private static final int VALID_DURATION_DAYS = 1;

    private static final String KEY_ALGORITHM = "RSA";

    private static final int KEY_SIZE = 4096;

    private static final int PROTECTION_PARAMETER_LENGTH = 32;

    private static final String PROTECTION_PARAMETER = generateProtectionParameter();

    private static final String KEYSTORE_FILE = "keystore.p12";

    private static final String TRUSTSTORE_FILE = "truststore.p12";

    private static final String KEYSTORE_TYPE = "PKCS12";

    private static Path persistentKeyStorePath;

    private static Path persistentTrustStorePath;

    private static KeyStore keyStore;

    private static KeyStore trustStore;

    private static SSLContext sslContext;

    /**
     * Configure KeyStores in provided directory and reuse existing files after initial generation
     *
     * @param keyStoreDirectory Directory where KeyStore and TrustStore should be stored
     */
    public static synchronized SSLContext configureKeyStores(final File keyStoreDirectory) {
        if (persistentKeyStorePath == null) {
            createKeyStores();
        }

        if (persistentKeyStorePath == null) {
            throw new IllegalStateException("KeyStore not provisioned");
        }
        if (persistentTrustStorePath == null) {
            throw new IllegalStateException("TrustStore not provisioned");
        }

        try {
            Files.copy(persistentKeyStorePath, Paths.get(keyStoreDirectory.getAbsolutePath(), KEYSTORE_FILE));
            Files.copy(persistentTrustStorePath, Paths.get(keyStoreDirectory.getAbsolutePath(), TRUSTSTORE_FILE));
        } catch (final IOException e) {
            throw new UncheckedIOException("KeyStore configuration failed", e);
        }

        return sslContext;
    }

    public static String getProtectionParameter() {
        return PROTECTION_PARAMETER;
    }

    private static void createKeyStores() {
        try {
            final KeyPair keyPair = getKeyPair();
            final X509Certificate certificate = new StandardCertificateBuilder(keyPair, DISTINGUISHED_NAME, Duration.ofDays(VALID_DURATION_DAYS))
                    .setDnsSubjectAlternativeNames(DNS_NAMES)
                    .build();

            persistentTrustStorePath = writeTrustStore(certificate);
            persistentTrustStorePath.toFile().deleteOnExit();

            persistentKeyStorePath = writeKeyStore(certificate, keyPair.getPrivate());
            persistentKeyStorePath.toFile().deleteOnExit();

            sslContext = new StandardSslContextBuilder()
                    .trustStore(trustStore)
                    .keyStore(keyStore)
                    .keyPassword(PROTECTION_PARAMETER.toCharArray())
                    .build();
        } catch (final Exception e) {
            throw new RuntimeException("KeyStore Creation Failed", e);
        }
    }

    private static Path writeKeyStore(final X509Certificate certificate, final PrivateKey privateKey) throws Exception {
        keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
        keyStore.load(null);

        final X509Certificate[] certificates = new X509Certificate[]{certificate};
        keyStore.setKeyEntry(HOSTNAME, privateKey, PROTECTION_PARAMETER.toCharArray(), certificates);

        final Path keyStorePath = Files.createTempFile(KEYSTORE_FILE, KEYSTORE_TYPE);
        try (final OutputStream outputStream = new FileOutputStream(keyStorePath.toFile())) {
            keyStore.store(outputStream, PROTECTION_PARAMETER.toCharArray());
        }
        return keyStorePath;
    }

    private static Path writeTrustStore(final X509Certificate certificate) throws Exception {
        trustStore = KeyStore.getInstance(KEYSTORE_TYPE);
        trustStore.load(null);
        trustStore.setCertificateEntry(HOSTNAME, certificate);

        final Path trustStorePath = Files.createTempFile(TRUSTSTORE_FILE, KEYSTORE_TYPE);
        try (final OutputStream outputStream = new FileOutputStream(trustStorePath.toFile())) {
            trustStore.store(outputStream, PROTECTION_PARAMETER.toCharArray());
        }
        return trustStorePath;
    }

    private static KeyPair getKeyPair() throws NoSuchAlgorithmException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        keyPairGenerator.initialize(KEY_SIZE);
        return keyPairGenerator.generateKeyPair();
    }

    private static String generateProtectionParameter() {
        final SecureRandom secureRandom = new SecureRandom();
        final byte[] nextBytes = new byte[PROTECTION_PARAMETER_LENGTH];
        secureRandom.nextBytes(nextBytes);
        return HexFormat.of().formatHex(nextBytes);
    }
}
