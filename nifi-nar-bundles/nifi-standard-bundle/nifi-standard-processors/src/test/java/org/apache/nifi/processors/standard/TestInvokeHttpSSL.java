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

package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.processors.standard.util.TestInvokeHttpCommon;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.TestServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Executes the same tests as TestInvokeHttp but with one-way SSL enabled.  The Jetty server created for these tests
 * will not require client certificates and the client will not use keystore properties in the SSLContextService.
 */
public class TestInvokeHttpSSL extends TestInvokeHttpCommon {

    protected static Map<String, String> sslProperties;
    protected static Map<String, String> serverSslProperties;
    protected static TlsConfiguration tlsConfiguration;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
        // useful for verbose logging output
        // don't commit this with this property enabled, or any 'mvn test' will be really verbose
        // System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard", "debug");

        // create TLS configuration with a new keystore and truststore
        tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();

        // create the SSL properties, which basically store keystore / truststore information
        // this is used by the StandardSSLContextService and the Jetty Server
        serverSslProperties = createServerSslProperties(false);
        sslProperties = createClientSslProperties(false);

        // create a Jetty server on a random port
        server = createServer();
        server.startServer();

        // Allow time for the server to start
        Thread.sleep(500);
        // this is the base url with the random port
        url = server.getSecureUrl();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (server != null) {
            server.shutdownServer();
        }

        if (tlsConfiguration != null) {
            try {
                if (StringUtils.isNotBlank(tlsConfiguration.getKeystorePath())) {
                    Files.deleteIfExists(Paths.get(tlsConfiguration.getKeystorePath()));
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a keystore: " + e.getMessage());
            }

            try {
                if (StringUtils.isNotBlank(tlsConfiguration.getTruststorePath())) {
                    Files.deleteIfExists(Paths.get(tlsConfiguration.getTruststorePath()));
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a truststore: " + e.getMessage());
            }
        }
    }

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(InvokeHTTP.class);
        final StandardSSLContextService sslService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslService, sslProperties);
        runner.enableControllerService(sslService);
        runner.setProperty(InvokeHTTP.PROP_SSL_CONTEXT_SERVICE, "ssl-context");

        // Allow time for the controller service to fully initialize
        Thread.sleep(500);

        // Provide more time to setup and run
        runner.setProperty(InvokeHTTP.PROP_READ_TIMEOUT, "30 secs");
        runner.setProperty(InvokeHTTP.PROP_CONNECT_TIMEOUT, "30 secs");

        server.clearHandlers();
    }

    @After
    public void after() {
        runner.shutdown();
    }

    static TestServer createServer() throws IOException {
        return new TestServer(serverSslProperties);
    }

    static Map<String, String> createServerSslProperties(boolean clientAuth) {
        final Map<String, String> map = new HashMap<>();
        // if requesting client auth then we must also provide a truststore
        if (clientAuth) {
            map.put(TestServer.NEED_CLIENT_AUTH, Boolean.toString(true));
            map.putAll(getTruststoreProperties());
        } else {
            map.put(TestServer.NEED_CLIENT_AUTH, Boolean.toString(false));
        }
        // keystore is always required for the server SSL properties
        map.putAll(getServerKeystoreProperties());

        return map;
    }

    static Map<String, String> createClientSslProperties(boolean clientAuth) {
        final Map<String, String> map = new HashMap<>();
        // if requesting client auth then we must provide a keystore
        if (clientAuth) {
            map.putAll(getClientKeystoreProperties());
        }
        // truststore is always required for the client SSL properties
        map.putAll(getTruststoreProperties());
        return map;
    }

    private static Map<String, String> getServerKeystoreProperties() {
        final Map<String, String> map = new HashMap<>();
        map.put(StandardSSLContextService.KEYSTORE.getName(), tlsConfiguration.getKeystorePath());
        map.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), tlsConfiguration.getKeystorePassword());
        map.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), tlsConfiguration.getKeystoreType().toString());
        return map;
    }

    private static Map<String, String> getClientKeystoreProperties() {
        final Map<String, String> map = new HashMap<>();
        map.put(StandardSSLContextService.KEYSTORE.getName(), tlsConfiguration.getKeystorePath());
        map.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), tlsConfiguration.getKeystorePassword());
        map.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), tlsConfiguration.getKeystoreType().toString());
        return map;
    }

    private static Map<String, String> getTruststoreProperties() {
        final Map<String, String> map = new HashMap<>();
        map.put(StandardSSLContextService.TRUSTSTORE.getName(), tlsConfiguration.getTruststorePath());
        // Commented this line to test passwordless truststores for NIFI-6770
        map.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), tlsConfiguration.getTruststorePassword());
        map.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), tlsConfiguration.getKeystoreType().toString());
        return map;
    }
}
