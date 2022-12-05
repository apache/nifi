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
package org.apache.nifi.security.ssl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;

import java.security.KeyStore;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MockitoExtension.class)
class StandardSslContextBuilderTest {
    private static final String TLS_PROTOCOL = "TLS";

    @Mock
    KeyStore trustStore;

    @Test
    void testBuild() {
        final StandardSslContextBuilder builder = new StandardSslContextBuilder();

        final SSLContext sslContext = builder.build();

        assertNotNull(sslContext);
    }

    @Test
    void testBuildProtocol() {
        final StandardSslContextBuilder builder = new StandardSslContextBuilder();
        builder.protocol(TLS_PROTOCOL);

        final SSLContext sslContext = builder.build();

        assertNotNull(sslContext);
    }

    @Test
    void testBuildKeyStore() throws Exception {
        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null);

        final StandardSslContextBuilder builder = new StandardSslContextBuilder();
        builder.keyStore(keyStore);

        final SSLContext sslContext = builder.build();

        assertNotNull(sslContext);
    }

    @Test
    void testBuildTrustStore() {
        final StandardSslContextBuilder builder = new StandardSslContextBuilder();
        builder.trustStore(trustStore);

        final SSLContext sslContext = builder.build();

        assertNotNull(sslContext);
    }
}
