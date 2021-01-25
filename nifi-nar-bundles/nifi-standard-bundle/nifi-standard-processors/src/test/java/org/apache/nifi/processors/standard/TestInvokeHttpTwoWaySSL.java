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

import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.junit.BeforeClass;

import javax.net.ssl.SSLContext;

/**
 * This is probably overkill but in keeping with the same pattern as the TestInvokeHttp and TestInvokeHttpSSL class,
 * we will execute the same tests using two-way SSL. The Jetty server created for these tests will require client
 * certificates and the client will utilize keystore properties in the SSLContextService.
 */
public class TestInvokeHttpTwoWaySSL extends TestInvokeHttpSSL {

    private static final String CLIENT_KEYSTORE_PATH = "src/test/resources/client-keystore.p12";
    private static final String CLIENT_KEYSTORE_PASSWORD = "passwordpassword";
    private static final KeystoreType CLIENT_KEYSTORE_TYPE = KeystoreType.PKCS12;

    private static final TlsConfiguration CLIENT_CONFIGURATION = new StandardTlsConfiguration(
            CLIENT_KEYSTORE_PATH,
            CLIENT_KEYSTORE_PASSWORD,
            CLIENT_KEYSTORE_TYPE,
            TRUSTSTORE_PATH,
            TRUSTSTORE_PASSWORD,
            TRUSTSTORE_TYPE
    );

    @BeforeClass
    public static void beforeClass() throws Exception {
        final SSLContext serverContext = SslContextFactory.createSslContext(SERVER_CONFIGURATION);
        configureServer(serverContext, ClientAuth.REQUIRED);
        clientSslContext = SslContextFactory.createSslContext(CLIENT_CONFIGURATION);
    }
}
