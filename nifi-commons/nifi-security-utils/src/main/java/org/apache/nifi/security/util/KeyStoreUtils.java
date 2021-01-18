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
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyStoreUtils {
    private static final Logger logger = LoggerFactory.getLogger(KeyStoreUtils.class);

    public static final String SUN_PROVIDER_NAME = "SUN";

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    /**
     * Returns the provider that will be used for the given keyStoreType
     *
     * @param keyStoreType the keyStoreType
     * @return the provider that will be used
     */
    public static String getKeyStoreProvider(String keyStoreType) {
        if (KeystoreType.PKCS12.toString().equalsIgnoreCase(keyStoreType)) {
            return BouncyCastleProvider.PROVIDER_NAME;
        } else if (KeystoreType.JKS.toString().equalsIgnoreCase(keyStoreType)) {
            return SUN_PROVIDER_NAME;
        }
        return null;
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
     * Returns an empty KeyStore intended for use as a TrustStore backed by the appropriate provider
     *
     * @param trustStoreType the trustStoreType
     * @return an empty KeyStore
     * @throws KeyStoreException if a KeyStore of the given type cannot be instantiated
     */
    public static KeyStore getTrustStore(String trustStoreType) throws KeyStoreException {
        if (KeystoreType.PKCS12.toString().equalsIgnoreCase(trustStoreType)) {
            logger.warn(trustStoreType + " truststores are deprecated.  " + KeystoreType.JKS.toString() + " is preferred.");
        }
        return getKeyStore(trustStoreType);
    }

    /**
     * Returns a loaded {@link KeyStore} given the provided configuration values.
     *
     * @param keystorePath     the file path to the keystore
     * @param keystorePassword the keystore password
     * @param keystoreType     the keystore type ({@code JKS} or {@code PKCS12})
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
     * Returns the intialized {@link KeyManagerFactory}.
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
     * @param keystoreType     the keystore type ({@code JKS} or {@code PKCS12})
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
     * @param truststoreType     the truststore type ({@code JKS} or {@code PKCS12})
     * @return the loaded truststore
     * @throws TlsException if there is a problem loading the truststore
     */
    public static KeyStore loadTrustStore(String truststorePath, char[] truststorePassword, String truststoreType) throws TlsException {
        final KeyStore trustStore;
        try {
            trustStore = KeyStoreUtils.getTrustStore(truststoreType);
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
     * Returns the intialized {@link TrustManagerFactory}.
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
     * @param truststoreType     the truststore type ({@code JKS} or {@code PKCS12})
     * @return the initialized trust manager factory
     * @throws TlsException if there is a problem initializing or reading from the truststore
     */
    public static TrustManagerFactory loadTrustManagerFactory(String truststorePath, String truststorePassword, String truststoreType) throws TlsException {
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
}
