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
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processors.standard.util.TestInvokeHttpCommon;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

/**
 * Executes the same tests as TestInvokeHttp but with one-way SSL enabled.  The Jetty server created for these tests
 * will not require client certificates and the client will not use keystore properties in the SSLContextService.
 */
public class TestInvokeHttpSSL extends TestInvokeHttpCommon {

    private static final String HTTP_CONNECT_TIMEOUT = "30 s";
    private static final String HTTP_READ_TIMEOUT = "30 s";

    protected static TlsConfiguration serverConfiguration;

    private static SSLContext truststoreSslContext;
    private static TlsConfiguration truststoreConfiguration;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // generate new keystore and truststore
        serverConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();

        truststoreConfiguration = new StandardTlsConfiguration(
                null,
                null,
                null,
                serverConfiguration.getTruststorePath(),
                serverConfiguration.getTruststorePassword(),
                serverConfiguration.getTruststoreType()
        );

        final SSLContext serverContext = SslContextFactory.createSslContext(serverConfiguration);
        configureServer(serverContext, ClientAuth.NONE);
        truststoreSslContext = SslContextFactory.createSslContext(truststoreConfiguration);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (serverConfiguration != null) {
            try {
                if (StringUtils.isNotBlank(serverConfiguration.getKeystorePath())) {
                    Files.deleteIfExists(Paths.get(serverConfiguration.getKeystorePath()));
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a keystore: " + e.getMessage(), e);
            }

            try {
                if (StringUtils.isNotBlank(serverConfiguration.getTruststorePath())) {
                    Files.deleteIfExists(Paths.get(serverConfiguration.getTruststorePath()));
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a truststore: " + e.getMessage(), e);
            }
        }
    }

    @Before
    public void before() throws Exception {
        final SSLContextService sslContextService = Mockito.mock(SSLContextService.class);
        final String serviceIdentifier = SSLContextService.class.getName();

        Mockito.when(sslContextService.getIdentifier()).thenReturn(serviceIdentifier);
        Mockito.when(sslContextService.createContext()).thenReturn(getClientSslContext());
        Mockito.when(sslContextService.createTlsConfiguration()).thenReturn(getClientConfiguration());

        runner = TestRunners.newTestRunner(InvokeHTTP.class);
        runner.addControllerService(serviceIdentifier, sslContextService);
        runner.enableControllerService(sslContextService);

        runner.setProperty(InvokeHTTP.PROP_SSL_CONTEXT_SERVICE, serviceIdentifier);
        runner.setProperty(InvokeHTTP.PROP_CONNECT_TIMEOUT, HTTP_CONNECT_TIMEOUT);
        runner.setProperty(InvokeHTTP.PROP_READ_TIMEOUT, HTTP_READ_TIMEOUT);
    }

    protected SSLContext getClientSslContext() {
        return truststoreSslContext;
    }

    protected TlsConfiguration getClientConfiguration() {
        return truststoreConfiguration;
    }
}
