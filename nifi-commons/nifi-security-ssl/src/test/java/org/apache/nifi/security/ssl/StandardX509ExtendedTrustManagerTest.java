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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class StandardX509ExtendedTrustManagerTest {
    private static final String AUTH_TYPE = "AUTH";

    private static final String ALIAS = "alias";

    private static final X509Certificate[] certificates = new X509Certificate[]{};

    @Mock
    private X509ExtendedTrustManager extendedTrustManager;

    @Mock
    private X509ExtendedTrustManager updatedTrustManager;

    @Mock
    private Socket socket;

    @Mock
    private SSLEngine sslEngine;

    private StandardX509ExtendedTrustManager manager;

    @BeforeEach
    void setManager() {
        manager = new StandardX509ExtendedTrustManager(extendedTrustManager);
    }

    @Test
    void testCheckClientTrusted() throws CertificateException {
        manager.checkClientTrusted(certificates, AUTH_TYPE);

        verify(extendedTrustManager).checkClientTrusted(eq(certificates), eq(AUTH_TYPE));
    }

    @Test
    void testCheckClientTrustedSocket() throws CertificateException {
        manager.checkClientTrusted(certificates, AUTH_TYPE, socket);

        verify(extendedTrustManager).checkClientTrusted(eq(certificates), eq(AUTH_TYPE), eq(socket));
    }


    @Test
    void testCheckClientTrustedEngine() throws CertificateException {
        manager.checkClientTrusted(certificates, AUTH_TYPE, sslEngine);

        verify(extendedTrustManager).checkClientTrusted(eq(certificates), eq(AUTH_TYPE), eq(sslEngine));
    }

    @Test
    void testCheckServerTrusted() throws CertificateException {
        manager.checkServerTrusted(certificates, AUTH_TYPE);

        verify(extendedTrustManager).checkServerTrusted(eq(certificates), eq(AUTH_TYPE));
    }

    @Test
    void testCheckServerTrustedSocket() throws CertificateException {
        manager.checkServerTrusted(certificates, AUTH_TYPE, socket);

        verify(extendedTrustManager).checkServerTrusted(eq(certificates), eq(AUTH_TYPE), eq(socket));
    }


    @Test
    void testCheckServerTrustedEngine() throws CertificateException {
        manager.checkServerTrusted(certificates, AUTH_TYPE, sslEngine);

        verify(extendedTrustManager).checkServerTrusted(eq(certificates), eq(AUTH_TYPE), eq(sslEngine));
    }

    @Test
    void testGetAcceptedIssuers() {
        manager.getAcceptedIssuers();

        verify(extendedTrustManager).getAcceptedIssuers();

        manager.setTrustManager(updatedTrustManager);
        manager.getAcceptedIssuers();

        verify(updatedTrustManager).getAcceptedIssuers();
    }
}
