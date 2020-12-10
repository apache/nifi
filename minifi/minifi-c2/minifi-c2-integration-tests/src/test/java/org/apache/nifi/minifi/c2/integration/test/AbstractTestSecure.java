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
import com.palantir.docker.compose.connection.DockerPort;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.minifi.commons.schema.ConfigSchema;
import org.apache.nifi.minifi.commons.schema.serialization.SchemaLoader;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandalone;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandaloneCommandLine;
import org.junit.Test;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class AbstractTestSecure extends AbstractTestUnsecure {
    public static final String C2_URL = "https://c2:10443/c2/config";

    private final DockerComposeRule docker;
    private final Path certificatesDirectory;
    private final SSLContext trustSslContext;

    protected AbstractTestSecure(DockerComposeRule docker, Path certificatesDirectory, SSLContext trustSslContext) {
        this.docker = docker;
        this.certificatesDirectory = certificatesDirectory;
        this.trustSslContext = trustSslContext;
    }

    @Override
    protected String getConfigUrl(DockerComposeRule docker) {
        return C2_URL;
    }

    public static SSLContext initCertificates(Path certificatesDirectory, List<String> serverHostnames) throws Exception {
        List<String> toolkitCommandLine = new ArrayList<>(Arrays.asList("-O", "-o", certificatesDirectory.toFile().getAbsolutePath(),
                "-C", "CN=user1", "-C", "CN=user2", "-C", "CN=user3", "-C", "CN=user4", "-S", "badKeystorePass", "-K", "badKeyPass", "-P", "badTrustPass"));
        for (String serverHostname : serverHostnames) {
            toolkitCommandLine.add("-n");
            toolkitCommandLine.add(serverHostname);
        }
        Files.createDirectories(certificatesDirectory);
        TlsToolkitStandaloneCommandLine tlsToolkitStandaloneCommandLine = new TlsToolkitStandaloneCommandLine();
        tlsToolkitStandaloneCommandLine.parse(toolkitCommandLine.toArray(new String[toolkitCommandLine.size()]));
        new TlsToolkitStandalone().createNifiKeystoresAndTrustStores(tlsToolkitStandaloneCommandLine.createConfig());

        tlsToolkitStandaloneCommandLine = new TlsToolkitStandaloneCommandLine();
        tlsToolkitStandaloneCommandLine.parse(new String[]{"-O", "-o", certificatesDirectory.getParent().resolve("badCert").toFile().getAbsolutePath(), "-C", "CN=user3"});
        new TlsToolkitStandalone().createNifiKeystoresAndTrustStores(tlsToolkitStandaloneCommandLine.createConfig());

        final KeyStore trustStore = KeyStoreUtils.getKeyStore("jks");
        try (final InputStream trustStoreStream = new FileInputStream(certificatesDirectory.resolve("c2").resolve("truststore.jks").toFile().getAbsolutePath())) {
            trustStore.load(trustStoreStream, "badTrustPass".toCharArray());
        }
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        TlsConfiguration tlsConfiguration = new StandardTlsConfiguration(
                null, null, null,
                certificatesDirectory.resolve("c2").resolve("truststore.jks").toFile().getAbsolutePath(), "badTrustPass", KeystoreType.JKS);


        return SslContextFactory.createSslContext(tlsConfiguration);
    }

    @Test
    public void testNoClientCert() throws Exception {
        assertReturnCode("", trustSslContext, 403);
        assertReturnCode("?class=raspi2", trustSslContext, 403);
        assertReturnCode("?class=raspi3", trustSslContext, 403);
    }

    @Test
    public void testUser1() throws Exception {
        SSLContext sslContext = loadSslContext("user1");

        assertReturnCode("", sslContext, 403);

        ConfigSchema configSchema = assertReturnCode("?class=raspi2", sslContext, 200);
        assertEquals("raspi2.v1", configSchema.getFlowControllerProperties().getName());

        assertReturnCode("?class=raspi3", sslContext, 403);
    }

    @Test
    public void testUser2() throws Exception {
        SSLContext sslContext = loadSslContext("user2");

        assertReturnCode("", sslContext, 403);
        assertReturnCode("?class=raspi2", sslContext, 403);

        ConfigSchema configSchema = assertReturnCode("?class=raspi3", sslContext, 200);
        assertEquals("raspi3.v2", configSchema.getFlowControllerProperties().getName());
    }

    @Test
    public void testUser3() throws Exception {
        SSLContext sslContext = loadSslContext("user3");

        assertReturnCode("", sslContext, 400);

        ConfigSchema configSchema = assertReturnCode("?class=raspi2", sslContext, 200);
        assertEquals("raspi2.v1", configSchema.getFlowControllerProperties().getName());

        configSchema = assertReturnCode("?class=raspi3", sslContext, 200);
        assertEquals("raspi3.v2", configSchema.getFlowControllerProperties().getName());
    }

    @Test(expected = IOException.class)
    public void testUser3WrongCA() throws Exception {
        assertReturnCode("?class=raspi3", loadSslContext("user3", certificatesDirectory.getParent().resolve("badCert")), 403);
    }

    @Test
    public void testUser4() throws Exception {
        SSLContext sslContext = loadSslContext("user4");

        assertReturnCode("", sslContext, 403);
        assertReturnCode("?class=raspi2", sslContext, 403);
        assertReturnCode("?class=raspi3", sslContext, 403);
    }

    protected SSLContext loadSslContext(String username) throws GeneralSecurityException, IOException {
        return loadSslContext(username, certificatesDirectory);
    }

    protected SSLContext loadSslContext(String username, Path directory) throws GeneralSecurityException, IOException {
        String keystorePasswd;
        try (InputStream inputStream = Files.newInputStream(directory.resolve("CN=" + username + ".password"))) {
            keystorePasswd = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
        TlsConfiguration tlsConfiguration = new StandardTlsConfiguration(
                directory.resolve("CN=" + username + ".p12").toFile().getAbsolutePath(),
                keystorePasswd,
                KeystoreType.PKCS12,
                certificatesDirectory.resolve("c2").resolve("truststore.jks").toFile().getAbsolutePath(),
                "badTrustPass",
                KeystoreType.JKS);

        return SslContextFactory.createSslContext(tlsConfiguration);
    }

    protected ConfigSchema assertReturnCode(String query, SSLContext sslContext, int expectedReturnCode) throws Exception {
        HttpsURLConnection httpsURLConnection = openUrlConnection(C2_URL + query, sslContext);
        try {
            assertEquals(expectedReturnCode, httpsURLConnection.getResponseCode());
            if (expectedReturnCode == 200) {
                return SchemaLoader.loadConfigSchemaFromYaml(httpsURLConnection.getInputStream());
            }
        } finally {
            httpsURLConnection.disconnect();
        }
        return null;
    }

    protected HttpsURLConnection openUrlConnection(String url, SSLContext sslContext) throws IOException {
        DockerPort dockerPort = docker.containers().container("squid").port(3128);
        HttpsURLConnection httpURLConnection = (HttpsURLConnection) new URL(url).openConnection(
                new Proxy(Proxy.Type.HTTP, new InetSocketAddress(dockerPort.getIp(), dockerPort.getExternalPort())));
        httpURLConnection.setSSLSocketFactory(sslContext.getSocketFactory());
        return httpURLConnection;
    }

    @Override
    protected HttpURLConnection openSuperUserUrlConnection(String url) throws IOException {
        try {
            return openUrlConnection(url, loadSslContext("user3"));
        } catch (GeneralSecurityException e) {
            throw new IOException(e);
        }
    }
}
