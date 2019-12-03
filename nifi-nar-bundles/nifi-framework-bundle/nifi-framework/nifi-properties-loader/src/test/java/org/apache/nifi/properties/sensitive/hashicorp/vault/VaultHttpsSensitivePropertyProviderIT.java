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
    package org.apache.nifi.properties.sensitive.hashicorp.vault;

    import junit.framework.Assert;
    import org.junit.AfterClass;
    import org.junit.Before;
    import org.junit.BeforeClass;
    import org.junit.ClassRule;
    import org.junit.Test;
    import org.junit.rules.TemporaryFolder;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;
    import org.springframework.core.io.FileSystemResource;
    import org.springframework.vault.authentication.SimpleSessionManager;
    import org.springframework.vault.authentication.TokenAuthentication;
    import org.springframework.vault.client.VaultEndpoint;
    import org.springframework.vault.config.ClientHttpRequestFactoryFactory;
    import org.springframework.vault.core.VaultTemplate;
    import org.springframework.vault.support.ClientOptions;
    import org.springframework.vault.support.SslConfiguration;
    import org.testcontainers.containers.GenericContainer;
    import org.testcontainers.containers.wait.strategy.Wait;
    import org.testcontainers.images.builder.ImageFromDockerfile;

    import java.io.File;
    import java.io.FileInputStream;
    import java.io.FileOutputStream;
    import java.io.IOException;
    import java.net.URI;
    import java.security.KeyStore;
    import java.security.KeyStoreException;
    import java.security.NoSuchAlgorithmException;
    import java.security.cert.CertificateException;
    import java.security.cert.CertificateFactory;
    import java.security.cert.X509Certificate;

    /**
     * These are the HTTPS versions of the tests in {@link VaultHttpSensitivePropertyProviderIT}.
     * <p>
     * This class leverages those methods by setting various Vault SSL system properties before running each test.
     * Vault server configuration, test set-up and tear-down, etc., are all handled by the base class.
     */
    public class VaultHttpsSensitivePropertyProviderIT extends VaultHttpSensitivePropertyProviderIT {
        private static final Logger logger = LoggerFactory.getLogger(VaultHttpsSensitivePropertyProviderIT.class);
        private static final int vaultPort = 8300;
        private static String vaultServerKeyStoreHostCopy;

        @ClassRule
        public static TemporaryFolder tempFolder = new TemporaryFolder(new File("/tmp/"));
        private static SslConfiguration.KeyStoreConfiguration keyStoreConfiguration;
        private static SslConfiguration sslConfiguration;

        @AfterClass
        public static void deleteTempFolder() {
            tempFolder.delete();
        }

        @BeforeClass
        public static void createTestContainer() {
            String vaultServerCertificateHostCopy;
            try {
                vaultServerCertificateHostCopy = tempFolder.newFile().getAbsolutePath();
                vaultServerKeyStoreHostCopy = tempFolder.newFile().getAbsolutePath();
            } catch (IOException e) {
                logger.error("Could not finish creating test container:", e);
                return;
            }
            logger.info("Have vault server certificate at " + vaultServerCertificateHostCopy);
            logger.info("Have keystore with vault server certificate at " + vaultServerKeyStoreHostCopy);

            // This creates a test container with our local Dockerfile and run script for vault.  The script will
            // create the certificate artifacts expected by these tests and then start the vault dev server.
            vaultContainer = new GenericContainer(
                    new ImageFromDockerfile()
                            .withFileFromClasspath("run-vault.sh", "vault_it/run-vault.sh")
                            .withFileFromClasspath("Dockerfile", "vault_it/Dockerfile"))
                    .withEnv("VAULT_DEV_ROOT_TOKEN_ID", vaultToken)
                    .withExposedPorts(vaultPort)
                    .waitingFor(Wait.forLogMessage("==> Vault server started.*", 1));

            vaultContainer.start();

            // Copy the server cert that was created during "run-vault.sh" at container startup:
            vaultContainer.copyFileFromContainer("/runtime/server.crt", vaultServerCertificateHostCopy);
            logger.info("Copied server cert to host path: " + vaultServerCertificateHostCopy);

            // After the server is started we can get the port and therefore we can construct the URL for the client:
            vaultUri = "https://" + vaultContainer.getContainerIpAddress() + ":" + vaultContainer.getMappedPort(vaultPort);

            try {
                KeyStore keyStore = KeyStore.getInstance("jks");
                CertificateFactory certificateFactory = CertificateFactory.getInstance("x.509");
                X509Certificate certificate = ((X509Certificate) (certificateFactory.generateCertificate(new FileInputStream(vaultServerCertificateHostCopy))));
                keyStore.load(null, null);
                keyStore.setCertificateEntry("localhost", certificate);
                keyStore.store(new FileOutputStream(vaultServerKeyStoreHostCopy), new char[0]);
                logger.info("Created server key store at host path: " + vaultServerKeyStoreHostCopy);
            } catch (final IOException | KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
                logger.error("Failed to created server ssl certificate keystore:", e);
                return;
            }

            keyStoreConfiguration = new SslConfiguration.KeyStoreConfiguration(new FileSystemResource(vaultServerKeyStoreHostCopy), null, KeyStore.getDefaultType());
            sslConfiguration = new SslConfiguration(keyStoreConfiguration, keyStoreConfiguration);

            // We're creating and using a dedicated vault client that's not tied to the SPP:
            vaultOperations = new VaultTemplate(
                    VaultEndpoint.from(URI.create(vaultUri)),
                    ClientHttpRequestFactoryFactory.create(new ClientOptions(), sslConfiguration),
                    new SimpleSessionManager(new TokenAuthentication(vaultToken)));

            configureVaultTestEnvironment(vaultOperations);
        }

        // Set the trust store location before each test; this allows the http client within the property provider to use the
        // vault server certificate created during container init.
        @Before
        public void setTrustStoreProperty() {
            System.setProperty("vault.ssl.trust-store", vaultServerKeyStoreHostCopy);
            System.setProperty("vault.ssl.key-store", vaultServerKeyStoreHostCopy);
        }

        @Test
        public void testHttpsShouldWorkWithTlsConfigs() {

            // vault tls + (null key store, null trust store)
            System.clearProperty("vault.ssl.trust-store");
            boolean failed = false;
            try {
                testShouldProtectAndUnprotectProperties();
            } catch (final Exception e) {
                failed = true;
            }
            Assert.assertTrue(failed);

            // vault tls + (client key store, null trust store)
            System.clearProperty("vault.ssl.trust-store");
            System.setProperty("vault.ssl.key-store", vaultServerKeyStoreHostCopy);
            failed = false;
            try {
                testShouldProtectAndUnprotectProperties();
            } catch (final Exception e) {
                failed = true;
            }
            Assert.assertTrue(failed);

            // vault tls + (client key store, trust store)
            System.setProperty("vault.ssl.trust-store", vaultServerKeyStoreHostCopy);
            System.setProperty("vault.ssl.key-store", vaultServerKeyStoreHostCopy);
            failed = false;
            try {
                testShouldProtectAndUnprotectProperties();
            } catch (final Exception e) {
                failed = true;
            }
            Assert.assertFalse(failed);

            // vault tls + (null key store, trust store)
            System.setProperty("vault.ssl.trust-store", vaultServerKeyStoreHostCopy);
            System.clearProperty("vault.ssl.key-store");
            failed = false;
            try {
                testShouldProtectAndUnprotectProperties();
            } catch (final Exception e) {
                failed = true;
            }
            // Assert.assertTrue(failed);
        }
    }
