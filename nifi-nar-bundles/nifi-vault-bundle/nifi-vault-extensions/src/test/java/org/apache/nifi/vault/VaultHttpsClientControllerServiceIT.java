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
package org.apache.nifi.vault;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.vault.service.VaultHttpClientControllerService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.vault.authentication.SimpleSessionManager;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.ClientHttpRequestFactoryFactory;
import org.springframework.vault.client.VaultEndpoint;
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
 * This class adds HTTPS-specific setup and test cases to the corresponding HTTP test class {@link VaultHttpClientControllerServiceIT}.
 */
public class VaultHttpsClientControllerServiceIT extends VaultHttpClientControllerServiceIT {
    private static final Logger logger = LoggerFactory.getLogger(VaultHttpsClientControllerServiceIT.class);
    private static String vaultServerKeyStoreHostCopy;

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder(new File("/tmp/"));

    @AfterClass
    public static void deleteTempFolder() {
        tempFolder.delete();
    }

    @BeforeClass
    public static void createTestContainer() {
        String vaultServerCertificateHostCopy;
        final int vaultPort = 8300;

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
            keyStore.store(new FileOutputStream(vaultServerKeyStoreHostCopy), "password".toCharArray());
            logger.info("Created server key store at host path: " + vaultServerKeyStoreHostCopy);
        } catch (final IOException | KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
            logger.error("Failed to created server ssl certificate keystore:", e);
            return;
        }

        // We're creating and using a dedicated vault client that's not tied to the service:
        SslConfiguration.KeyStoreConfiguration keyStoreConfiguration = new SslConfiguration.KeyStoreConfiguration(new FileSystemResource(vaultServerKeyStoreHostCopy), null, KeyStore.getDefaultType());
        SslConfiguration sslConfiguration = new SslConfiguration(keyStoreConfiguration, keyStoreConfiguration);
        vaultOperations = new VaultTemplate(
                VaultEndpoint.from(URI.create(vaultUri)),
                ClientHttpRequestFactoryFactory.create(new ClientOptions(), sslConfiguration),
                new SimpleSessionManager(new TokenAuthentication(vaultToken)));

        configureVaultTestEnvironment(vaultOperations);
    }

    TestRunner createTestRunner(String authType, String uri, String path, VaultHttpClientControllerService service) throws InitializationException {
        TestRunner runner = super.createTestRunner(authType, uri, path, service);
        SSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, vaultServerKeyStoreHostCopy);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "password");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.enableControllerService(sslContextService);
        runner.setProperty(service, VaultHttpClientControllerService.SSL_CONTEXT_SERVICE, "ssl-context");
        return runner;
    }
}
