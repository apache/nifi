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

import org.apache.nifi.processors.standard.util.TestPutTCPCommon;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.ssl.SSLContextService;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;

public class TestPutTcpSSL extends TestPutTCPCommon {
    private static final String TLS_PROTOCOL = "TLSv1.2";

    private static SSLContext sslContext;

    @BeforeClass
    public static void configureServices() throws TlsException {
        final TlsConfiguration configuration = new StandardTlsConfiguration(
                "src/test/resources/keystore.jks",
                "passwordpassword",
                "passwordpassword",
                KeystoreType.JKS,
                "src/test/resources/truststore.jks",
                "passwordpassword",
                KeystoreType.JKS,
                TLS_PROTOCOL
        );
        sslContext = SslContextFactory.createSslContext(configuration);
    }

    public TestPutTcpSSL() {
        super();
        serverSocketFactory = sslContext.getServerSocketFactory();
    }

    @Override
    public void configureProperties(String host, int port, String outgoingMessageDelimiter, boolean connectionPerFlowFile, boolean expectValid) throws InitializationException {
        runner.setProperty(PutTCP.HOSTNAME, host);
        runner.setProperty(PutTCP.PORT, Integer.toString(port));

        final SSLContextService sslContextService = Mockito.mock(SSLContextService.class);
        final String serviceIdentifier = SSLContextService.class.getName();
        Mockito.when(sslContextService.getIdentifier()).thenReturn(serviceIdentifier);
        Mockito.when(sslContextService.createContext()).thenReturn(sslContext);

        runner.addControllerService(serviceIdentifier, sslContextService);
        runner.enableControllerService(sslContextService);
        runner.setProperty(PutTCP.SSL_CONTEXT_SERVICE, serviceIdentifier);

        if (outgoingMessageDelimiter != null) {
            runner.setProperty(PutTCP.OUTGOING_MESSAGE_DELIMITER, outgoingMessageDelimiter);
        }
        runner.setProperty(PutTCP.CONNECTION_PER_FLOWFILE, String.valueOf(connectionPerFlowFile));

        if (expectValid) {
            runner.assertValid();
        } else {
            runner.assertNotValid();
        }
    }
}
