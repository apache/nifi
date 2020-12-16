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
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TlsConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * This is probably overkill but in keeping with the same pattern as the TestInvokeHttp and TestInvokeHttpSSL class,
 * we will execute the same tests using two-way SSL. The Jetty server created for these tests will require client
 * certificates and the client will utilize keystore properties in the SSLContextService.
 */
public class TestInvokeHttpTwoWaySSL extends TestInvokeHttpSSL {

    private static TlsConfiguration serverConfig;
    private static SSLContext clientSslContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // generate new keystore and truststore
        serverConfig = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();

        final SSLContext serverContext = SslContextFactory.createSslContext(serverConfig);
        configureServer(serverContext, ClientAuth.REQUIRED);
        clientSslContext = SslContextFactory.createSslContext(serverConfig);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (serverConfig != null) {
            try {
                if (StringUtils.isNotBlank(serverConfig.getKeystorePath())) {
                    Files.deleteIfExists(Paths.get(serverConfig.getKeystorePath()));
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a keystore: " + e.getMessage(), e);
            }

            try {
                if (StringUtils.isNotBlank(serverConfig.getTruststorePath())) {
                    Files.deleteIfExists(Paths.get(serverConfig.getTruststorePath()));
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a truststore: " + e.getMessage(), e);
            }
        }
    }

    @Override
    protected SSLContext getClientSslContext() {
        return clientSslContext;
    }

    @Override
    protected TlsConfiguration getClientConfiguration() {
        return serverConfig;
    }

}
