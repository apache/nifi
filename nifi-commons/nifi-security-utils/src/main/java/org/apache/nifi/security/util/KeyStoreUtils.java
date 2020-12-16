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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyStoreUtils {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreUtils.class);

    public static final String SUN_PROVIDER_NAME = "SUN";
    private static final String JKS_EXT = ".jks";
    private static final String PKCS12_EXT = ".p12";
    private static final String BCFKS_EXT = ".bcfks";
    private static final String KEY_ALIAS = "nifi-key";
    private static final String CERT_ALIAS = "nifi-cert";
    private static final String CERT_DN = "CN=localhost";
    private static final String KEY_ALGORITHM = "RSA";
    private static final String SIGNING_ALGORITHM = "SHA256withRSA";
    private static final int CERT_DURATION_DAYS = 365;
    private static final int PASSWORD_LENGTH = 16;
    private static final String TEST_KEYSTORE_PREFIX = "test-keystore-";
    private static final String TEST_TRUSTSTORE_PREFIX = "test-truststore-";

    private static final String KEYSTORE_ERROR_MSG = "There was an error creating a Keystore.";
    private static final String TRUSTSTORE_ERROR_MSG = "There was an error creating a Truststore.";

    private static final Map<String, String> KEY_STORE_TYPE_PROVIDERS = new HashMap<>();
    private static final Map<KeystoreType, String> KEY_STORE_EXTENSIONS = new HashMap<>();

    static {
        Security.addProvider(new BouncyCastleProvider());

        KEY_STORE_TYPE_PROVIDERS.put(KeystoreType.BCFKS.getType(), BouncyCastleProvider.PROVIDER_NAME);
        KEY_STORE_TYPE_PROVIDERS.put(KeystoreType.PKCS12.getType(), BouncyCastleProvider.PROVIDER_NAME);
        KEY_STORE_TYPE_PROVIDERS.put(KeystoreType.JKS.getType(), SUN_PROVIDER_NAME);
    }

    static {
        KEY_STORE_EXTENSIONS.put(KeystoreType.JKS, JKS_EXT);
        KEY_STORE_EXTENSIONS.put(KeystoreType.PKCS12, PKCS12_EXT);
        KEY_STORE_EXTENSIONS.put(KeystoreType.BCFKS, BCFKS_EXT);
    }

    /**
     * Returns the provider that will be used for the given keyStoreType
     *
     * @param keyStoreType the keyStoreType
     * @return Key Store Provider Name or null when not found
     */
    public static String getKeyStoreProvider(String keyStoreType) {
        final String storeType = StringUtils.upperCase(keyStoreType);
        return KEY_STORE_TYPE_PROVIDERS.get(storeType);
    }

    /**
     * Returns an empty KeyStore backed by the appropriate provider
     *
     * @param keyStoreType the keyStoreType
     * @return an empty KeyStore
     * @throws KeyStoreException if a KeyStore of the given type cannot be instantiated
     */
    public static KeyStore getKeyStore(String keyStoreType) throws KeyStoreException {
        String keyStoreProvider = getKeyStoreProvider(keyStoreType);
        if (StringUtils.isNotEmpty(keyStoreProvider)) {
            try {
                return KeyStore.getInstance(keyStoreType, keyStoreProvider);
            } catch (Exception e) {
                logger.error("Unable to load " + keyStoreProvider + " " + keyStoreType
                        + " keystore.  This may cause issues getting trusted CA certificates as well as Certificate Chains for use in TLS.", e);
            }
        }
        return KeyStore.getInstance(keyStoreType);
    }

    /**
     * Returns a loaded {@link KeyStore} given the provided configuration values.
     *
     * @param keystorePath     the file path to the keystore
     * @param keystorePassword the keystore password
     * @param keystoreType     the keystore type
     * @return the loaded keystore
     * @throws TlsException if there is a problem loading the keystore
     */
    public static KeyStore loadKeyStore(String keystorePath, char[] keystorePassword, String keystoreType) throws TlsException {
        final KeyStore keyStore;
        try {
            keyStore = KeyStoreUtils.getKeyStore(keystoreType);
            try (final InputStream keyStoreStream = new FileInputStream(keystorePath)) {
                keyStore.load(keyStoreStream, keystorePassword);
            }
            return keyStore;
        } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
            logger.error("Encountered an error loading keystore: {}", e.getLocalizedMessage());
            throw new TlsException("Error loading keystore", e);
        }
    }

    /**
     * Creates a temporary default Keystore and Truststore and returns it wrapped in a TLS configuration.
     *
     * @return a {@link org.apache.nifi.security.util.TlsConfiguration}
     */
    public static TlsConfiguration createTlsConfigAndNewKeystoreTruststore() throws IOException, GeneralSecurityException {
        return createTlsConfigAndNewKeystoreTruststore(new StandardTlsConfiguration());
    }

    /**
     * Creates a temporary Keystore and Truststore and returns it wrapped in a new TLS configuration with the given values.
     *
     * @param tlsConfiguration a {@link org.apache.nifi.security.util.TlsConfiguration}
     * @return a {@link org.apache.nifi.security.util.TlsConfiguration}
     */
    public static TlsConfiguration createTlsConfigAndNewKeystoreTruststore(final TlsConfiguration tlsConfiguration) throws IOException, GeneralSecurityException {
        final Path keyStorePath;
        final String keystorePassword = StringUtils.isNotBlank(tlsConfiguration.getKeystorePassword()) ? tlsConfiguration.getKeystorePassword() : generatePassword();
        final KeystoreType keystoreType = tlsConfiguration.getKeystoreType() != null ? tlsConfiguration.getKeystoreType() : KeystoreType.PKCS12;
        final String keyPassword = StringUtils.isNotBlank(tlsConfiguration.getKeyPassword()) ? tlsConfiguration.getKeyPassword() : keystorePassword;
        final Path trustStorePath;
        final String truststorePassword = StringUtils.isNotBlank(tlsConfiguration.getTruststorePassword()) ? tlsConfiguration.getTruststorePassword() : generatePassword();
        final KeystoreType truststoreType = tlsConfiguration.getTruststoreType() != null ? tlsConfiguration.getTruststoreType() : KeystoreType.PKCS12;

        // Create temporary Keystore file
        try {
            keyStorePath = generateTempKeystorePath(keystoreType);
        } catch (IOException e) {
            logger.error(KEYSTORE_ERROR_MSG, e);
            throw new UncheckedIOException(KEYSTORE_ERROR_MSG, e);
        }

        // Create temporary Truststore file
        try {
            trustStorePath = generateTempTruststorePath(truststoreType);
        } catch (IOException e) {
            logger.error(TRUSTSTORE_ERROR_MSG, e);
            throw new UncheckedIOException(TRUSTSTORE_ERROR_MSG, e);
        }

        // Create X509 Certificate
        final X509Certificate clientCert = createKeyStoreAndGetX509Certificate(KEY_ALIAS, keystorePassword, keyPassword, keyStorePath.toString(), keystoreType);

        // Create Truststore
        createTrustStore(clientCert, CERT_ALIAS, truststorePassword, trustStorePath.toString(), truststoreType);

        return new StandardTlsConfiguration(
                keyStorePath.toString(),
                keystorePassword,
                keyPassword,
                keystoreType,
                trustStorePath.toString(),
                truststorePassword,
                truststoreType,
                TlsPlatform.getLatestProtocol());
    }

    /**
     * Returns the {@link KeyManagerFactory} from the provided {@link KeyStore} object, initialized with the key or keystore password.
     *
     * @param keyStore         the loaded keystore
     * @param keystorePassword the keystore password
     * @param keyPassword      the key password
     * @return the key manager factory
     * @throws TlsException if there is a problem initializing or reading from the keystore
     */
    public static KeyManagerFactory getKeyManagerFactoryFromKeyStore(KeyStore keyStore, char[] keystorePassword, char[] keyPassword) throws TlsException {
        try {
            final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            if (keyPassword == null) {
                keyManagerFactory.init(keyStore, keystorePassword);
            } else {
                keyManagerFactory.init(keyStore, keyPassword);
            }
            return keyManagerFactory;
        } catch (NoSuchAlgorithmException | KeyStoreException | UnrecoverableKeyException e) {
            logger.error("Encountered an error loading keystore: {}", e.getLocalizedMessage());
            throw new TlsException("Error loading keystore", e);
        }
    }

    /**
     * Returns the initialized {@link KeyManagerFactory}.
     *
     * @param tlsConfiguration the TLS configuration
     * @return the initialized key manager factory
     * @throws TlsException if there is a problem initializing or reading from the keystore
     */
    public static KeyManagerFactory loadKeyManagerFactory(TlsConfiguration tlsConfiguration) throws TlsException {
        return loadKeyManagerFactory(tlsConfiguration.getKeystorePath(), tlsConfiguration.getKeystorePassword(),
                tlsConfiguration.getFunctionalKeyPassword(), tlsConfiguration.getKeystoreType().getType());
    }

    /**
     * Returns the initialized {@link KeyManagerFactory}.
     *
     * @param keystorePath     the file path to the keystore
     * @param keystorePassword the keystore password
     * @param keyPassword      the key password
     * @param keystoreType     the keystore type
     * @return the initialized key manager factory
     * @throws TlsException if there is a problem initializing or reading from the keystore
     */
    public static KeyManagerFactory loadKeyManagerFactory(String keystorePath, String keystorePassword, String keyPassword, String keystoreType) throws TlsException {
        if (StringUtils.isEmpty(keystorePassword)) {
            throw new IllegalArgumentException("The keystore password cannot be null or empty");
        }
        final char[] keystorePasswordChars = keystorePassword.toCharArray();
        final char[] keyPasswordChars = (StringUtils.isNotEmpty(keyPassword)) ? keyPassword.toCharArray() : keystorePasswordChars;
        KeyStore keyStore = loadKeyStore(keystorePath, keystorePasswordChars, keystoreType);
        return getKeyManagerFactoryFromKeyStore(keyStore, keystorePasswordChars, keyPasswordChars);
    }

    /**
     * Returns a loaded {@link KeyStore} (acting as a truststore) given the provided configuration values.
     *
     * @param truststorePath     the file path to the truststore
     * @param truststorePassword the truststore password
     * @param truststoreType     the truststore type
     * @return the loaded truststore
     * @throws TlsException if there is a problem loading the truststore
     */
    public static KeyStore loadTrustStore(String truststorePath, char[] truststorePassword, String truststoreType) throws TlsException {
        final KeyStore trustStore;
        try {
            trustStore = KeyStoreUtils.getKeyStore(truststoreType);
            try (final InputStream trustStoreStream = new FileInputStream(truststorePath)) {
                trustStore.load(trustStoreStream, truststorePassword);
            }
            return trustStore;
        } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException e) {
            logger.error("Encountered an error loading truststore: {}", e.getLocalizedMessage());
            throw new TlsException("Error loading truststore", e);
        }
    }

    /**
     * Returns the {@link TrustManagerFactory} from the provided {@link KeyStore} object, initialized.
     *
     * @param trustStore the loaded truststore
     * @return the trust manager factory
     * @throws TlsException if there is a problem initializing or reading from the truststore
     */
    public static TrustManagerFactory getTrustManagerFactoryFromTrustStore(KeyStore trustStore) throws TlsException {
        try {
            final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            return trustManagerFactory;
        } catch (NoSuchAlgorithmException | KeyStoreException e) {
            logger.error("Encountered an error loading truststore: {}", e.getLocalizedMessage());
            throw new TlsException("Error loading truststore", e);
        }
    }

    /**
     * Returns the initialized {@link TrustManagerFactory}.
     *
     * @param tlsConfiguration the TLS configuration
     * @return the initialized trust manager factory
     * @throws TlsException if there is a problem initializing or reading from the truststore
     */
    public static TrustManagerFactory loadTrustManagerFactory(TlsConfiguration tlsConfiguration) throws TlsException {
        return loadTrustManagerFactory(tlsConfiguration.getTruststorePath(), tlsConfiguration.getTruststorePassword(), tlsConfiguration.getTruststoreType().getType());
    }

    /**
     * Returns the initialized {@link TrustManagerFactory}.
     *
     * @param truststorePath     the file path to the truststore
     * @param truststorePassword the truststore password
     * @param truststoreType     the truststore type
     * @return the initialized trust manager factory
     * @throws TlsException if there is a problem initializing or reading from the truststore
     */
    public static TrustManagerFactory loadTrustManagerFactory(String truststorePath, String truststorePassword, String truststoreType) throws TlsException {
        // Bouncy Castle PKCS12 type requires a password
        if (truststoreType.equalsIgnoreCase(KeystoreType.PKCS12.getType()) && StringUtils.isBlank(truststorePassword)) {
            throw new IllegalArgumentException("A PKCS12 Truststore Type requires a password");
        }

        // Legacy truststore passwords can be empty
        final char[] truststorePasswordChars = StringUtils.isNotBlank(truststorePassword) ? truststorePassword.toCharArray() : null;
        KeyStore trustStore = loadTrustStore(truststorePath, truststorePasswordChars, truststoreType);
        return getTrustManagerFactoryFromTrustStore(trustStore);
    }

    /**
     * Returns true if the given keystore can be loaded using the given keystore type and password. Returns false otherwise.
     *
     * @param keystore     the keystore to validate
     * @param keystoreType the type of the keystore
     * @param password     the password to access the keystore
     * @return true if valid; false otherwise
     */
    public static boolean isStoreValid(final URL keystore, final KeystoreType keystoreType, final char[] password) {

        if (keystore == null) {
            throw new IllegalArgumentException("Keystore may not be null");
        } else if (keystoreType == null) {
            throw new IllegalArgumentException("Keystore type may not be null");
        } else if (password == null) {
            throw new IllegalArgumentException("Password may not be null");
        }

        BufferedInputStream bis = null;
        final KeyStore ks;
        try {

            // Load the keystore
            bis = new BufferedInputStream(keystore.openStream());
            ks = KeyStoreUtils.getKeyStore(keystoreType.name());
            ks.load(bis, password);

            return true;

        } catch (Exception e) {
            return false;
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close input stream", ioe);
                }
            }
        }
    }

    /**
     * Returns true if the given keystore can be loaded using the given keystore type and password and the default
     * (first) alias can be retrieved with the key-specific password. Returns false otherwise.
     *
     * @param keystore     the keystore to validate
     * @param keystoreType the type of the keystore
     * @param password     the password to access the keystore
     * @param keyPassword  the password to access the specific key
     * @return true if valid; false otherwise
     */
    public static boolean isKeyPasswordCorrect(final URL keystore, final KeystoreType keystoreType, final char[] password, final char[] keyPassword) {

        if (keystore == null) {
            throw new IllegalArgumentException("Keystore may not be null");
        } else if (keystoreType == null) {
            throw new IllegalArgumentException("Keystore type may not be null");
        } else if (password == null) {
            throw new IllegalArgumentException("Password may not be null");
        }

        BufferedInputStream bis = null;
        final KeyStore ks;
        try {

            // Load the keystore
            bis = new BufferedInputStream(keystore.openStream());
            ks = KeyStoreUtils.getKeyStore(keystoreType.name());
            ks.load(bis, password);

            // Determine the default alias
            String alias = ks.aliases().nextElement();
            try {
                Key privateKeyEntry = ks.getKey(alias, keyPassword);
                return true;
            } catch (UnrecoverableKeyException e) {
                logger.warn("Tried to access a key in keystore " + keystore + " with a key password that failed");
                return false;
            }
        } catch (Exception e) {
            return false;
        } finally {
            if (bis != null) {
                try {
                    bis.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close input stream", ioe);
                }
            }
        }
    }

    public static String sslContextToString(SSLContext sslContext) {
        return new ToStringBuilder(sslContext)
                .append("protocol", sslContext.getProtocol())
                .append("provider", sslContext.getProvider().toString())
                .toString();
    }

    public static String sslParametersToString(SSLParameters sslParameters) {
        return new ToStringBuilder(sslParameters)
                .append("protocols", sslParameters.getProtocols())
                .append("wantClientAuth", sslParameters.getWantClientAuth())
                .append("needClientAuth", sslParameters.getNeedClientAuth())
                .toString();
    }

    public static String sslServerSocketToString(SSLServerSocket sslServerSocket) {
        return new ToStringBuilder(sslServerSocket)
                .append("enabledProtocols", sslServerSocket.getEnabledProtocols())
                .append("needClientAuth", sslServerSocket.getNeedClientAuth())
                .append("wantClientAuth", sslServerSocket.getWantClientAuth())
                .append("useClientMode", sslServerSocket.getUseClientMode())
                .toString();
    }

    /**
     * Loads the Keystore and returns a X509 Certificate with the given values.
     *
     * @param alias            the certificate alias
     * @param keyStorePassword the keystore password
     * @param keyPassword      the key password
     * @param keyStorePath     the keystore path
     * @param keyStoreType     the keystore type
     * @return a {@link X509Certificate}
     */
    private static X509Certificate createKeyStoreAndGetX509Certificate(
            final String alias, final String keyStorePassword, final String keyPassword, final String keyStorePath,
            final KeystoreType keyStoreType) throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {

        try (final FileOutputStream outputStream = new FileOutputStream(keyStorePath)) {
            final KeyPair keyPair = KeyPairGenerator.getInstance(KEY_ALGORITHM).generateKeyPair();

            final X509Certificate selfSignedCert = CertificateUtils.generateSelfSignedX509Certificate(
                    keyPair, CERT_DN, SIGNING_ALGORITHM, CERT_DURATION_DAYS
            );

            final KeyStore keyStore = loadEmptyKeyStore(keyStoreType);
            keyStore.setKeyEntry(alias, keyPair.getPrivate(), keyPassword.toCharArray(), new Certificate[]{selfSignedCert});
            keyStore.store(outputStream, keyStorePassword.toCharArray());

            return selfSignedCert;
        }
    }

    /**
     * Loads the Truststore with the given values.
     *
     * @param cert           the certificate
     * @param alias          the certificate alias
     * @param password       the truststore password
     * @param path           the truststore path
     * @param truststoreType the truststore type
     */
    private static void createTrustStore(final X509Certificate cert,
                                         final String alias, final String password, final String path, final KeystoreType truststoreType)
            throws KeyStoreException, NoSuchAlgorithmException, CertificateException {

        try (final FileOutputStream outputStream = new FileOutputStream(path)) {
            final KeyStore trustStore = loadEmptyKeyStore(truststoreType);
            trustStore.setCertificateEntry(alias, cert);
            trustStore.store(outputStream, password.toCharArray());
        } catch (IOException e) {
            throw new UncheckedIOException(TRUSTSTORE_ERROR_MSG, e);
        }
    }

    /**
     * Generates a temporary keystore file and returns the path.
     *
     * @param keystoreType the Keystore type
     * @return a Path
     */
    private static Path generateTempKeystorePath(KeystoreType keystoreType) throws IOException {
        return Files.createTempFile(TEST_KEYSTORE_PREFIX, getKeystoreExtension(keystoreType));
    }

    /**
     * Generates a temporary truststore file and returns the path.
     *
     * @param truststoreType the Truststore type
     * @return a Path
     */
    private static Path generateTempTruststorePath(KeystoreType truststoreType) throws IOException {
        return Files.createTempFile(TEST_TRUSTSTORE_PREFIX, getKeystoreExtension(truststoreType));
    }

    /**
     * Loads and returns an empty Keystore backed by the appropriate provider.
     *
     * @param keyStoreType the keystore type
     * @return an empty keystore
     * @throws KeyStoreException if a keystore of the given type cannot be instantiated
     */
    private static KeyStore loadEmptyKeyStore(KeystoreType keyStoreType) throws KeyStoreException, CertificateException, NoSuchAlgorithmException {
        final KeyStore keyStore;
        try {
            keyStore = KeyStore.getInstance(
                    Objects.requireNonNull(keyStoreType).getType());
            keyStore.load(null, null);
            return keyStore;
        } catch (IOException e) {
            logger.error("Encountered an error loading keystore: {}", e.getLocalizedMessage());
            throw new UncheckedIOException("Error loading keystore", e);
        }
    }

    /**
     * Returns the Keystore extension given the Keystore type.
     *
     * @param keystoreType the keystore type
     * @return the keystore extension
     */
    private static String getKeystoreExtension(KeystoreType keystoreType) {
        return KEY_STORE_EXTENSIONS.get(keystoreType);
    }

    /**
     * Generates a random Hex-encoded password.
     *
     * @return a password as a Hex-encoded String
     */
    private static String generatePassword() {
        final byte[] password = new byte[PASSWORD_LENGTH];
        new SecureRandom().nextBytes(password);
        return Hex.encodeHexString(password);
    }
}
