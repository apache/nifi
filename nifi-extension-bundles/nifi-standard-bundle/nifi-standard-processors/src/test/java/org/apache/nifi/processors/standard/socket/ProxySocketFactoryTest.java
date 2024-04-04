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

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ProxySocketFactoryTest {

    @Test
    public void testCreateSocketNotConnected() {
        final Proxy.Type proxyType = Proxy.Type.SOCKS;
        final InetSocketAddress proxyAddress = new InetSocketAddress("localhost", 1080);
        final Proxy proxy = new Proxy(proxyType, proxyAddress);

        final ProxySocketFactory socketFactory = new ProxySocketFactory(proxy);
        final Socket socket = socketFactory.createSocket();

        assertNotNull(socket);
        assertFalse(socket.isConnected());
    }
}
