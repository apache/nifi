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

import java.net.Socket;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

import java.security.Principal;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class StandardX509ExtendedKeyManagerTest {
    private static final String KEY_TYPE = "RSA";

    private static final String ALIAS = "alias";

    private static final Principal[] ISSUERS = new Principal[]{};

    @Mock
    private X509ExtendedKeyManager extendedKeyManager;

    @Mock
    private X509ExtendedKeyManager updatedKeyManager;

    @Mock
    private Socket socket;

    @Mock
    private SSLEngine sslEngine;

    private StandardX509ExtendedKeyManager manager;

    @BeforeEach
    void setManager() {
        manager = new StandardX509ExtendedKeyManager(extendedKeyManager);
    }

    @Test
    void testGetClientAlias() {
        manager.getClientAliases(KEY_TYPE, ISSUERS);

        verify(extendedKeyManager).getClientAliases(eq(KEY_TYPE), eq(ISSUERS));
    }

    @Test
    void testChooseClientAlias() {
        final String[] keyTypes = new String[]{KEY_TYPE};

        manager.chooseClientAlias(keyTypes, ISSUERS, socket);

        verify(extendedKeyManager).chooseClientAlias(eq(keyTypes), eq(ISSUERS), eq(socket));
    }

    @Test
    void testChooseEngineClientAlias() {
        final String[] keyTypes = new String[]{KEY_TYPE};

        manager.chooseEngineClientAlias(keyTypes, ISSUERS, sslEngine);

        verify(extendedKeyManager).chooseEngineClientAlias(eq(keyTypes), eq(ISSUERS), eq(sslEngine));
    }

    @Test
    void testChooseServerAlias() {
        manager.chooseServerAlias(KEY_TYPE, ISSUERS, socket);

        verify(extendedKeyManager).chooseServerAlias(eq(KEY_TYPE), eq(ISSUERS), eq(socket));
    }

    @Test
    void testChooseEngineServerAlias() {
        manager.chooseEngineServerAlias(KEY_TYPE, ISSUERS, sslEngine);

        verify(extendedKeyManager).chooseEngineServerAlias(eq(KEY_TYPE), eq(ISSUERS), eq(sslEngine));
    }

    @Test
    void testGetPrivateKey() {
        manager.getPrivateKey(ALIAS);

        verify(extendedKeyManager).getPrivateKey(eq(ALIAS));
    }

    @Test
    void testGetCertificateChain() {
        manager.getCertificateChain(ALIAS);

        verify(extendedKeyManager).getCertificateChain(eq(ALIAS));
    }

    @Test
    void testGetServerAliases() {
        manager.getServerAliases(KEY_TYPE, ISSUERS);

        verify(extendedKeyManager).getServerAliases(KEY_TYPE, ISSUERS);

        manager.setKeyManager(updatedKeyManager);
        manager.getServerAliases(KEY_TYPE, ISSUERS);

        verify(updatedKeyManager).getServerAliases(KEY_TYPE, ISSUERS);
    }
}
