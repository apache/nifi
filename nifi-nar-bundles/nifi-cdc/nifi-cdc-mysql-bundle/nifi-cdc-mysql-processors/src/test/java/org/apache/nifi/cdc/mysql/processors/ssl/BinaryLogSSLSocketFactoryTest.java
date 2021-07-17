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
package org.apache.nifi.cdc.mysql.processors.ssl;

import org.junit.Test;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BinaryLogSSLSocketFactoryTest {

    private static final int PORT = 65000;

    @Test
    public void testCreateSocket() throws IOException {
        final SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        final BinaryLogSSLSocketFactory socketFactory = new BinaryLogSSLSocketFactory(sslSocketFactory);

        final Socket socket = mock(Socket.class);
        when(socket.isConnected()).thenReturn(true);
        final InetAddress address = InetAddress.getLoopbackAddress();
        when(socket.getInetAddress()).thenReturn(address);
        when(socket.getPort()).thenReturn(PORT);

        final SSLSocket sslSocket = socketFactory.createSocket(socket);
        assertNotNull("SSL Socket not found", sslSocket);
        assertEquals("Address not matched", address, sslSocket.getInetAddress());
        assertEquals("Port not matched", PORT, sslSocket.getPort());
    }

    @Test
    public void testCreateSocketException() {
        final SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        final BinaryLogSSLSocketFactory socketFactory = new BinaryLogSSLSocketFactory(sslSocketFactory);

        final Socket socket = mock(Socket.class);
        final InetAddress address = InetAddress.getLoopbackAddress();
        when(socket.getInetAddress()).thenReturn(address);
        when(socket.getPort()).thenReturn(PORT);

        assertThrows(IOException.class, () -> socketFactory.createSocket(socket));
    }
}
