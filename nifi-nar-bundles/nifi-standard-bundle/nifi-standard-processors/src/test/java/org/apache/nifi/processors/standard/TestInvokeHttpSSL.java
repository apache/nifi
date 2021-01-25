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

import org.apache.nifi.processors.standard.util.TestInvokeHttpCommon;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;

import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;

/**
 * Executes the same tests as TestInvokeHttp but with one-way SSL enabled.  The Jetty server created for these tests
 * will not require client certificates and the client will not use keystore properties in the SSLContextService.
 */
public class TestInvokeHttpSSL extends TestInvokeHttpCommon {

    protected static final String TRUSTSTORE_PATH = "src/test/resources/truststore.no-password.jks";
    protected static final String TRUSTSTORE_PASSWORD = "";
    protected static final KeystoreType TRUSTSTORE_TYPE = KeystoreType.JKS;

    private static final String KEYSTORE_PATH = "src/test/resources/keystore.jks";
    private static final String KEYSTORE_PASSWORD = "passwordpassword";
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS;

    private static final String HTTP_CONNECT_TIMEOUT = "30 s";
    private static final String HTTP_READ_TIMEOUT = "30 s";

    protected static final TlsConfiguration SERVER_CONFIGURATION = new StandardTlsConfiguration(
            KEYSTORE_PATH,
            KEYSTORE_PASSWORD,
            KEYSTORE_TYPE,
            TRUSTSTORE_PATH,
            TRUSTSTORE_PASSWORD,
            TRUSTSTORE_TYPE
    );

    protected static SSLContext clientSslContext;

    private static final TlsConfiguration CLIENT_CONFIGURATION = new StandardTlsConfiguration(
            null,
            null,
            null,
            TRUSTSTORE_PATH,
            TRUSTSTORE_PASSWORD,
            TRUSTSTORE_TYPE
    );

    @BeforeClass
    public static void beforeClass() throws Exception {
        final SSLContext serverContext = SslContextFactory.createSslContext(SERVER_CONFIGURATION);
        configureServer(serverContext, ClientAuth.NONE);
        clientSslContext = SslContextFactory.createSslContext(CLIENT_CONFIGURATION);
    }

    @Before
    public void before() throws Exception {
        final SSLContextService sslContextService = Mockito.mock(SSLContextService.class);
        final String serviceIdentifier = SSLContextService.class.getName();

        Mockito.when(sslContextService.getIdentifier()).thenReturn(serviceIdentifier);
        Mockito.when(sslContextService.createContext()).thenReturn(clientSslContext);
        Mockito.when(sslContextService.createTlsConfiguration()).thenReturn(CLIENT_CONFIGURATION);

        runner = TestRunners.newTestRunner(InvokeHTTP.class);
        runner.addControllerService(serviceIdentifier, sslContextService);
        runner.enableControllerService(sslContextService);

        runner.setProperty(InvokeHTTP.PROP_SSL_CONTEXT_SERVICE, serviceIdentifier);
        runner.setProperty(InvokeHTTP.PROP_CONNECT_TIMEOUT, HTTP_CONNECT_TIMEOUT);
        runner.setProperty(InvokeHTTP.PROP_READ_TIMEOUT, HTTP_READ_TIMEOUT);
    }
}
