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
import com.exceptionfactory.socketbroker.configuration.AuthenticationCredentials;
import com.exceptionfactory.socketbroker.configuration.BrokerConfiguration;
import com.exceptionfactory.socketbroker.configuration.ProxyType;
import com.exceptionfactory.socketbroker.configuration.StandardBrokerConfiguration;
import com.exceptionfactory.socketbroker.configuration.StandardUsernamePasswordAuthenticationCredentials;
import org.apache.nifi.proxy.ProxyConfiguration;

import javax.net.SocketFactory;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Objects;

/**
 * Standard implementation of Socket Factory Provider support authenticated or unauthenticated SOCKS or HTTP proxies
 */
public class StandardSocketFactoryProvider implements SocketFactoryProvider {
    /**
     * Get Socket Factory returns ProxySocketFactory without credentials and BrokeredSocketFactory with credentials
     *
     * @param proxyConfiguration Proxy Configuration required
     * @return Socket Factory
     */
    @Override
    public SocketFactory getSocketFactory(final ProxyConfiguration proxyConfiguration) {
        Objects.requireNonNull(proxyConfiguration, "Proxy Configuration required");

        final String userName = proxyConfiguration.getProxyUserName();
        final SocketFactory socketFactory;
        if (userName == null) {
            final Proxy proxy = proxyConfiguration.createProxy();
            socketFactory = new ProxySocketFactory(proxy);
        } else {
            final Proxy.Type proxyType = proxyConfiguration.getProxyType();
            final ProxyType brokerProxyType = Proxy.Type.SOCKS == proxyType ? ProxyType.SOCKS5 : ProxyType.HTTP_CONNECT;
            final InetSocketAddress proxySocketAddress = new InetSocketAddress(proxyConfiguration.getProxyServerHost(), proxyConfiguration.getProxyServerPort());

            final String proxyPassword = proxyConfiguration.getProxyUserPassword();
            final char[] brokerProxyPassword = proxyPassword == null ? new char[]{} : proxyPassword.toCharArray();
            final AuthenticationCredentials credentials = new StandardUsernamePasswordAuthenticationCredentials(userName, brokerProxyPassword);

            final BrokerConfiguration brokerConfiguration = new StandardBrokerConfiguration(brokerProxyType, proxySocketAddress, credentials);
            socketFactory = new BrokeredSocketFactory(brokerConfiguration, SocketFactory.getDefault());
        }
        return socketFactory;
    }
}
