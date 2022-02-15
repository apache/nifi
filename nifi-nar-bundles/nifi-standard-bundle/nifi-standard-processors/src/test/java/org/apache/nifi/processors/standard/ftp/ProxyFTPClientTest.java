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
package org.apache.nifi.processors.standard.ftp;

import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.SocketFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ProxyFTPClientTest {
    @Mock
    private SocketFactory socketFactory;

    @Mock
    private Socket socket;

    @Captor
    private ArgumentCaptor<InetSocketAddress> socketAddressCaptor;

    private static final String HOST = "host.unresolved";

    private static final String WELCOME_REPLY = "220 Welcome";

    private int port;

    private ProxyFTPClient client;

    @BeforeEach
    public void setClient() {
        port = NetworkUtils.getAvailableTcpPort();
        client = new ProxyFTPClient(socketFactory);
    }

    @Test
    public void testConnect() throws IOException {
        when(socketFactory.createSocket()).thenReturn(socket);
        when(socket.getInputStream()).thenReturn(new ByteArrayInputStream(WELCOME_REPLY.getBytes(StandardCharsets.US_ASCII)));
        when(socket.getOutputStream()).thenReturn(new ByteArrayOutputStream());

        client.connect(HOST, port);

        verify(socket).connect(socketAddressCaptor.capture(), anyInt());

        final InetSocketAddress socketAddress = socketAddressCaptor.getValue();
        assertNotNull(socketAddress);
        assertEquals(HOST, socketAddress.getHostString());
        assertEquals(port, socketAddress.getPort());
    }
}
