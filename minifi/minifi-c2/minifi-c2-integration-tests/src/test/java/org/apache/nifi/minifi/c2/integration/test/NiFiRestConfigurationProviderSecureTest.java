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

package org.apache.nifi.minifi.c2.integration.test;

import com.palantir.docker.compose.DockerComposeRule;
import org.apache.nifi.minifi.c2.integration.test.health.HttpsStatusCodeHealthCheck;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Arrays;

public class NiFiRestConfigurationProviderSecureTest extends AbstractTestSecure {
    private static SSLSocketFactory healthCheckSocketFactory;
    private static Path certificatesDirectory;
    private static SSLContext trustSslContext;

    // Not annotated as rule because we need to generate certificatesDirectory first
    public static DockerComposeRule docker = DockerComposeRule.builder()
            .file("target/test-classes/docker-compose-NiFiRestConfigurationProviderSecureTest.yml")
            .waitingForServices(Arrays.asList("squid", "mocknifi"),
                    new HttpsStatusCodeHealthCheck(container -> "https://mocknifi:8443/", containers -> containers.get(0), containers -> containers.get(1), () -> {
                        Path c2 = certificatesDirectory.resolve("c2");
                        try {
                            TlsConfiguration tlsConfiguration = new StandardTlsConfiguration(
                            c2.resolve("keystore.jks").toFile().getAbsolutePath(),
                                    "badKeystorePass",
                                    "badKeyPass", KeystoreType.JKS,
                                    c2.resolve("truststore.jks").toFile().getAbsolutePath(),
                                    "badTrustPass",
                                    KeystoreType.JKS);

                            return SslContextFactory.createSslContext(tlsConfiguration).getSocketFactory();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }, 200))
            .waitingForServices(Arrays.asList("squid", "c2"),
                    new HttpsStatusCodeHealthCheck(container -> C2_URL, containers -> containers.get(0), containers -> containers.get(1), () -> healthCheckSocketFactory, 403))
            .build();

    public NiFiRestConfigurationProviderSecureTest() {
        super(docker, certificatesDirectory, trustSslContext);
    }

    /**
     * Generates certificates with the tls-toolkit and then starts up the docker compose file
     */
    @BeforeClass
    public static void initCertificates() throws Exception {
        certificatesDirectory = Paths.get(NiFiRestConfigurationProviderSecureTest.class.getClassLoader()
                .getResource("docker-compose-NiFiRestConfigurationProviderSecureTest.yml").getFile()).getParent().toAbsolutePath().resolve("certificates-NiFiRestConfigurationProviderSecureTest");
        trustSslContext = initCertificates(certificatesDirectory, Arrays.asList("c2", "mocknifi"));
        healthCheckSocketFactory = trustSslContext.getSocketFactory();

        KeyStore mockNiFiKeyStore = KeyStore.getInstance("JKS");
        try (InputStream inputStream = Files.newInputStream(certificatesDirectory.resolve("mocknifi").resolve("keystore.jks"))) {
            mockNiFiKeyStore.load(inputStream, "badKeystorePass".toCharArray());
        }
        try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(Files.newOutputStream(certificatesDirectory.resolve("mocknifi").resolve("cert.pem"))))) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(mockNiFiKeyStore.getKey(TlsToolkitStandalone.NIFI_KEY, "badKeyPass".toCharArray())));
            for (Certificate certificate : mockNiFiKeyStore.getCertificateChain(TlsToolkitStandalone.NIFI_KEY)) {
                pemWriter.writeObject(new JcaMiscPEMGenerator(certificate));
            }
        }

        KeyStore mockNiFiTrustStore = KeyStore.getInstance("JKS");
        try (InputStream inputStream = Files.newInputStream(certificatesDirectory.resolve("mocknifi").resolve("truststore.jks"))) {
            mockNiFiTrustStore.load(inputStream, "badTrustPass".toCharArray());
        }
        try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(Files.newOutputStream(certificatesDirectory.resolve("mocknifi").resolve("ca.pem"))))) {
            pemWriter.writeObject(new JcaMiscPEMGenerator(mockNiFiTrustStore.getCertificate(TlsToolkitStandalone.NIFI_CERT)));
        }

        docker.before();
    }

    @AfterClass
    public static void cleanup() {
        docker.after();
    }

    @Before
    public void setup() {
        super.setup(docker);
    }
}
