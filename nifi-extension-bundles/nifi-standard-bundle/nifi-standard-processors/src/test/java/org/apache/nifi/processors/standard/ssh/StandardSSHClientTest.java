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
package org.apache.nifi.processors.standard.ssh;

import net.schmizz.sshj.DefaultConfig;
import org.apache.nifi.processors.standard.socket.ProxySocketFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardSSHClientTest {
    private static final String HOSTNAME = "localhost";

    private static final int PORT = 22;

    private static final int PROXY_PORT = 8080;

    @Test
    void testMakeInetSocketAddressResolved() throws IOException {
        try (StandardSSHClient client = new StandardSSHClient(new DefaultConfig())) {
            final InetSocketAddress address = client.makeInetSocketAddress(HOSTNAME, PORT);

            assertFalse(address.isUnresolved());
            assertEquals(PORT, address.getPort());
            assertEquals(HOSTNAME, address.getHostString());
        }
    }

    @Test
    void testMakeInetSocketAddressUnresolved() throws IOException {
        final InetSocketAddress proxyAddress = InetSocketAddress.createUnresolved(HOSTNAME, PROXY_PORT);
        final Proxy proxy = new Proxy(Proxy.Type.HTTP, proxyAddress);
        final ProxySocketFactory proxySocketFactory = new ProxySocketFactory(proxy);

        try (StandardSSHClient client = new StandardSSHClient(new DefaultConfig())) {
            client.setSocketFactory(proxySocketFactory);
            final InetSocketAddress address = client.makeInetSocketAddress(HOSTNAME, PORT);

            assertTrue(address.isUnresolved());
            assertEquals(PORT, address.getPort());
            assertEquals(HOSTNAME, address.getHostString());
        }
    }
}
