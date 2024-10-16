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

import org.apache.nifi.distributed.cache.client.MapCacheClientService;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.operations.MapOperation;
import org.apache.nifi.distributed.cache.protocol.ProtocolVersion;
import org.apache.nifi.distributed.cache.server.codec.CacheVersionRequestHandler;
import org.apache.nifi.distributed.cache.server.codec.CacheVersionResponseEncoder;
import org.apache.nifi.distributed.cache.server.codec.MapCacheRequestDecoder;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestMapCacheClientService {
    private static final String LOCALHOST = "127.0.0.1";

    private static final int MAX_REQUEST_LENGTH = 64;

    private final Serializer<String> serializer = new StringSerializer();

    private int port;

    private EventServer server;

    private TestRunner runner;

    @BeforeEach
    public void setRunner() throws UnknownHostException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        final InetAddress serverAddress = InetAddress.getByName(LOCALHOST);
        final NettyEventServerFactory serverFactory = new NettyEventServerFactory(serverAddress, port, TransportProtocol.TCP);
        serverFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        serverFactory.setShutdownTimeout(ShutdownQuietPeriod.QUICK.getDuration());
        final ComponentLog log = runner.getLogger();
        final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(ProtocolVersion.V3.value());

        serverFactory.setHandlerSupplier(() -> Arrays.asList(
                new CacheVersionResponseEncoder(),
                new MapCacheRequestDecoder(log, MAX_REQUEST_LENGTH, MapOperation.values()),
                new CacheVersionRequestHandler(log, versionNegotiator)
        ));
        server = serverFactory.getEventServer();
        port = server.getListeningPort();
    }

    @AfterEach
    public void shutdownServer() {
        server.shutdown();
    }

    /**
     * Service will hold request long enough for client timeout to be triggered, thus causing the request to fail.
     */
    @Test
    public void testClientTimeoutOnServerNetworkFailure() throws InitializationException {
        final String clientId = MapCacheClientService.class.getSimpleName();
        final MapCacheClientService clientService = new MapCacheClientService();

        runner.addControllerService(clientId, clientService);
        runner.setProperty(clientService, MapCacheClientService.HOSTNAME, LOCALHOST);
        runner.setProperty(clientService, MapCacheClientService.PORT, String.valueOf(port));
        runner.setProperty(clientService, MapCacheClientService.COMMUNICATIONS_TIMEOUT, "500 ms");
        runner.enableControllerService(clientService);
        runner.assertValid();

        try {
            assertThrows(SocketTimeoutException.class, () -> clientService.put("key", "value", serializer, serializer));
        } finally {
            runner.disableControllerService(clientService);
        }
    }

    private static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(final String value, final OutputStream output) throws IOException {
            output.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
