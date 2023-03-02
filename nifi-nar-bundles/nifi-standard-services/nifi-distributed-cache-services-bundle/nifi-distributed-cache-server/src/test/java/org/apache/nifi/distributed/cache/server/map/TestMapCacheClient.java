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

import org.apache.commons.lang3.SerializationException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService;
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
import org.apache.nifi.processor.Processor;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestMapCacheClient {
    private int port;
    private EventServer server;
    private final Serializer<String> stringSerializer = new StringSerializer();

    @BeforeEach
    public void setRunner() throws UnknownHostException {
        final TestRunner runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));
        port = NetworkUtils.getAvailableTcpPort();

        final NettyEventServerFactory serverFactory = new NettyEventServerFactory(
                InetAddress.getByName("127.0.0.1"), port, TransportProtocol.TCP);
        serverFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        serverFactory.setShutdownTimeout(ShutdownQuietPeriod.QUICK.getDuration());
        final ComponentLog log = runner.getLogger();
        final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(
                ProtocolVersion.V3.value(), ProtocolVersion.V2.value(), ProtocolVersion.V1.value());

        serverFactory.setHandlerSupplier(() -> Arrays.asList(
                new CacheVersionResponseEncoder(),
                new MapCacheRequestDecoder(log, 64, MapOperation.values()),
                new CacheVersionRequestHandler(log, versionNegotiator)
        ));
        server = serverFactory.getEventServer();
    }

    @AfterEach
    public void shutdownServer() {
        server.shutdown();
    }

    /**
     * Service will hold request long enough for client timeout to be triggered, thus causing the request to fail.
     */
    @Test
    public void testClientTimeoutOnServerNetworkFailure() throws InitializationException, IOException {
        final DistributedMapCacheClientService client = new DistributedMapCacheClientService();
        final MockControllerServiceInitializationContext clientInitContext1 =
                new MockControllerServiceInitializationContext(client, "client");
        client.initialize(clientInitContext1);

        final Map<PropertyDescriptor, String> clientProperties = new HashMap<>();
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME, "127.0.0.1");
        clientProperties.put(DistributedMapCacheClientService.PORT, String.valueOf(port));
        clientProperties.put(DistributedMapCacheClientService.COMMUNICATIONS_TIMEOUT, "500 ms");

        final MockConfigurationContext clientContext =
                new MockConfigurationContext(clientProperties, clientInitContext1.getControllerServiceLookup());
        client.onEnabled(clientContext);

        try {
            final String key = "test-timeout";
            assertThrows(SocketTimeoutException.class, () -> client.put(key, "value1", stringSerializer, stringSerializer));
        } finally {
            client.close();
        }
    }

    private static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(final String value, final OutputStream output) throws SerializationException, IOException {
            output.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
