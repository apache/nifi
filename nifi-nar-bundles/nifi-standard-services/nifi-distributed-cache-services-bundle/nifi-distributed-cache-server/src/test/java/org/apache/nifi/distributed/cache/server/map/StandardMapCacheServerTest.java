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
package org.apache.nifi.distributed.cache.server.map;

import org.apache.nifi.distributed.cache.operations.MapOperation;
import org.apache.nifi.distributed.cache.protocol.ProtocolHandshake;
import org.apache.nifi.distributed.cache.protocol.ProtocolVersion;
import org.apache.nifi.distributed.cache.server.EvictionPolicy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class StandardMapCacheServerTest {
    private static final String IDENTIFIER = StandardMapCacheServer.class.getSimpleName();

    private static final SSLContext SSL_CONTEXT_DISABLED = null;

    private static final int MAX_CACHE_ENTRIES = 32;

    private static final EvictionPolicy EVICTION_POLICY = EvictionPolicy.FIFO;

    private static final File PERSISTENCE_PATH_DISABLED = null;

    private static final int MAX_READ_LENGTH = 4096;

    private static final String LOCALHOST = "127.0.0.1";

    private static final byte[] HEADER = new byte[]{'N', 'i', 'F', 'i'};

    private static final String KEY = String.class.getSimpleName();

    private static final int KEY_NOT_FOUND = 0;

    @Mock
    ComponentLog log;

    StandardMapCacheServer server;

    @BeforeEach
    void setServer() throws IOException {
        final int port = NetworkUtils.getAvailableTcpPort();
        server = new StandardMapCacheServer(
                log,
                IDENTIFIER,
                SSL_CONTEXT_DISABLED,
                port,
                MAX_CACHE_ENTRIES,
                EVICTION_POLICY,
                PERSISTENCE_PATH_DISABLED,
                MAX_READ_LENGTH
        );
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop();
    }

    @Test
    void testSocketContainsKeyValueDelayed() throws IOException, InterruptedException {
        try (
                final Socket socket = new Socket(LOCALHOST, server.getPort());
                final InputStream inputStream = socket.getInputStream();
                final OutputStream outputStream = socket.getOutputStream();
                final DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
                ) {
            outputStream.write(HEADER);
            dataOutputStream.writeInt(ProtocolVersion.V3.value());

            final int protocolResponse = inputStream.read();
            assertEquals(ProtocolHandshake.RESOURCE_OK, protocolResponse);

            dataOutputStream.writeUTF(MapOperation.CONTAINS_KEY.value());

            // Delay writing key to simulate slow network connection
            TimeUnit.MILLISECONDS.sleep(200);

            dataOutputStream.writeUTF(KEY);

            final int read = inputStream.read();
            assertEquals(KEY_NOT_FOUND, read);
        }
    }
}
