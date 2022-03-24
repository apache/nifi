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
package org.apache.nifi.processors.standard.socket;

import com.exceptionfactory.socketbroker.BrokeredSocketFactory;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.SocketFactory;
import java.net.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StandardSocketFactoryProviderTest {
    private static final String HOST = "localhost";

    private static final int PORT = 1080;

    private static final String USERNAME = "user";

    private static final String PASSWORD = "password";

    private StandardSocketFactoryProvider provider;

    @BeforeEach
    public void setProvider() {
        provider = new StandardSocketFactoryProvider();
    }

    @Test
    public void testGetSocketFactoryWithoutCredentials() {
        final ProxyConfiguration proxyConfiguration = new ProxyConfiguration();
        proxyConfiguration.setProxyType(Proxy.Type.SOCKS);
        proxyConfiguration.setProxyServerHost(HOST);
        proxyConfiguration.setProxyServerPort(PORT);

        final SocketFactory socketFactory = provider.getSocketFactory(proxyConfiguration);
        assertEquals(ProxySocketFactory.class, socketFactory.getClass());
    }

    @Test
    public void testGetSocketFactoryWithUsername() {
        final ProxyConfiguration proxyConfiguration = new ProxyConfiguration();
        proxyConfiguration.setProxyType(Proxy.Type.SOCKS);
        proxyConfiguration.setProxyServerHost(HOST);
        proxyConfiguration.setProxyServerPort(PORT);
        proxyConfiguration.setProxyUserName(USERNAME);

        final SocketFactory socketFactory = provider.getSocketFactory(proxyConfiguration);
        assertEquals(BrokeredSocketFactory.class, socketFactory.getClass());
    }

    @Test
    public void testGetSocketFactoryWithUsernamePassword() {
        final ProxyConfiguration proxyConfiguration = new ProxyConfiguration();
        proxyConfiguration.setProxyType(Proxy.Type.SOCKS);
        proxyConfiguration.setProxyServerHost(HOST);
        proxyConfiguration.setProxyServerPort(PORT);
        proxyConfiguration.setProxyUserName(USERNAME);
        proxyConfiguration.setProxyUserPassword(PASSWORD);

        final SocketFactory socketFactory = provider.getSocketFactory(proxyConfiguration);
        assertEquals(BrokeredSocketFactory.class, socketFactory.getClass());
    }
}
