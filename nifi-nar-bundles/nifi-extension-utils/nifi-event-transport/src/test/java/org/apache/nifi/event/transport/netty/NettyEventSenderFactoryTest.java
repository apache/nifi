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
package org.apache.nifi.event.transport.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.time.Duration;

import static org.junit.Assert.assertThrows;

public class NettyEventSenderFactoryTest {
    private static final String ADDRESS = "127.0.0.1";

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);

    private static final int SINGLE_THREAD = 1;

    @Test
    public void testSendEventTcpException() throws Exception {
        final int port = NetworkUtils.getAvailableTcpPort();
        final NettyEventSenderFactory<ByteBuf> factory = new NettyEventSenderFactory<>(ADDRESS, port, TransportProtocol.TCP);
        factory.setTimeout(DEFAULT_TIMEOUT);
        factory.setWorkerThreads(SINGLE_THREAD);
        factory.setThreadNamePrefix(NettyEventSenderFactoryTest.class.getSimpleName());
        final SSLContext sslContext = SSLContext.getDefault();
        factory.setSslContext(sslContext);
        try (final EventSender<ByteBuf> eventSender = factory.getEventSender()) {
            assertThrows(EventException.class, () -> eventSender.sendEvent(ByteBufAllocator.DEFAULT.buffer()));
        }
    }

    @Test
    public void testSendEventCloseUdp() throws Exception {
        final int port = NetworkUtils.getAvailableUdpPort();
        final NettyEventSenderFactory<ByteBuf> factory = new NettyEventSenderFactory<>(ADDRESS, port, TransportProtocol.UDP);
        factory.setTimeout(DEFAULT_TIMEOUT);
        factory.setWorkerThreads(SINGLE_THREAD);
        final EventSender<ByteBuf> eventSender = factory.getEventSender();
        eventSender.sendEvent(ByteBufAllocator.DEFAULT.buffer());
        eventSender.close();
    }
}
