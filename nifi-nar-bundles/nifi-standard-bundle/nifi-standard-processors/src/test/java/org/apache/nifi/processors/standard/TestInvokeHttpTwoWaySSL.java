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
import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.util.StringUtils;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

/**
 * This is probably overkill but in keeping with the same pattern as the TestInvokeHttp and TestInvokeHttpSSL class,
 * we will execute the same tests using two-way SSL. The Jetty server created for these tests will require client
 * certificates and the client will utilize keystore properties in the SSLContextService.
 */
public class TestInvokeHttpTwoWaySSL extends TestInvokeHttpSSL {

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
        serverSslProperties = createServerSslProperties(true);
        sslProperties = createClientSslProperties(true);

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
