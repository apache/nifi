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

package org.apache.nifi.web.server;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class JettyServerTest {
    @Test
    public void testConfigureSslContextFactoryWithKeystorePasswordAndKeyPassword() {
        // Expect that if we set both passwords, KeyStore password is used for KeyStore, Key password is used for Key Manager
        String testKeystorePassword = "testKeystorePassword";
        String testKeyPassword = "testKeyPassword";

        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, testKeystorePassword);
        addProps.put(NiFiProperties.SECURITY_KEY_PASSWD, testKeyPassword);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory contextFactory = mock(SslContextFactory.class);

        JettyServer.configureSslContextFactory(contextFactory, nifiProperties);

        verify(contextFactory).setKeyStorePassword(testKeystorePassword);
        verify(contextFactory).setKeyManagerPassword(testKeyPassword);
    }

    @Test
    public void testConfigureSslContextFactoryWithKeyPassword() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        // Expect that with no KeyStore password, we will only need to set Key Manager Password
        String testKeyPassword = "testKeyPassword";

        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.SECURITY_KEY_PASSWD, testKeyPassword);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory contextFactory = mock(SslContextFactory.class);

        JettyServer.configureSslContextFactory(contextFactory, nifiProperties);

        verify(contextFactory).setKeyManagerPassword(testKeyPassword);
        verify(contextFactory, never()).setKeyStorePassword(anyString());
    }

    @Test
    public void testConfigureSslContextFactoryWithKeystorePassword() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        // Expect that with no KeyPassword, we use the same one from the KeyStore
        String testKeystorePassword = "testKeystorePassword";

        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.SECURITY_KEYSTORE_PASSWD, testKeystorePassword);
        NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, addProps);
        SslContextFactory contextFactory = mock(SslContextFactory.class);

        JettyServer.configureSslContextFactory(contextFactory, nifiProperties);

        verify(contextFactory).setKeyStorePassword(testKeystorePassword);
        verify(contextFactory).setKeyManagerPassword(testKeystorePassword);
    }

}
