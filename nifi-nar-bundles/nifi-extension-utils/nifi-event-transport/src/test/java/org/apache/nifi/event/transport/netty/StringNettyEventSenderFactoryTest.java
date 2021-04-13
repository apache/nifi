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

import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.configuration.LineEnding;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TlsConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

@RunWith(MockitoJUnitRunner.class)
public class StringNettyEventSenderFactoryTest {
    private static final String ADDRESS = "127.0.0.1";

    private static final int MAX_FRAME_LENGTH = 1024;

    private static final long TIMEOUT_SECONDS = 5;

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(TIMEOUT_SECONDS);

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final String MESSAGE = String.class.getName();

    private static final String DELIMITER = "\n";

    private static final int SINGLE_THREAD = 1;

    @Mock
    private ComponentLog log;

    @Test
    public void testSendEventTcpEventException() throws Exception {
        final int port = NetworkUtils.getAvailableTcpPort();

        final BlockingQueue<ByteArrayMessage> messages = new LinkedBlockingQueue<>();
        final NettyEventServerFactory serverFactory = getEventServerFactory(port, messages);
        final EventServer eventServer = serverFactory.getEventServer();
        final NettyEventSenderFactory<String> senderFactory = getEventSenderFactory(port);
        try (final EventSender<String> sender = senderFactory.getEventSender()) {
            eventServer.shutdown();
            assertThrows(EventException.class, () -> sender.sendEvent(MESSAGE));
        } finally {
            eventServer.shutdown();
        }
    }

    @Test
    public void testSendEventTcp() throws Exception {
        final int port = NetworkUtils.getAvailableTcpPort();

        final BlockingQueue<ByteArrayMessage> messages = new LinkedBlockingQueue<>();
        final NettyEventServerFactory serverFactory = getEventServerFactory(port, messages);
        final EventServer eventServer = serverFactory.getEventServer();
        final NettyEventSenderFactory<String> senderFactory = getEventSenderFactory(port);
        try (final EventSender<String> sender = senderFactory.getEventSender()) {
            sender.sendEvent(MESSAGE);
        } finally {
            eventServer.shutdown();
        }

        assertMessageReceived(messages);
    }

    @Test
    public void testSendEventTcpSslContextConfigured() throws Exception {
        final int port = NetworkUtils.getAvailableTcpPort();

        final NettyEventSenderFactory<String> senderFactory = getEventSenderFactory(port);
        final SSLContext sslContext = getSslContext();
        senderFactory.setSslContext(sslContext);

        final BlockingQueue<ByteArrayMessage> messages = new LinkedBlockingQueue<>();
        final NettyEventServerFactory serverFactory = getEventServerFactory(port, messages);
        serverFactory.setSslContext(sslContext);
        serverFactory.setClientAuth(ClientAuth.NONE);
        final EventServer eventServer = serverFactory.getEventServer();

        try (final EventSender<String> eventSender = senderFactory.getEventSender()) {
            eventSender.sendEvent(MESSAGE);
        } finally {
            eventServer.shutdown();
        }

        assertMessageReceived(messages);
    }

    private void assertMessageReceived(final BlockingQueue<ByteArrayMessage> messages) throws InterruptedException {
        final ByteArrayMessage messageReceived = messages.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull("Message not received", messageReceived);
        final String eventReceived = new String(messageReceived.getMessage(), CHARSET);
        assertEquals("Message not matched", MESSAGE, eventReceived);
        assertEquals("Sender not matched", ADDRESS, messageReceived.getSender());
    }

    private NettyEventSenderFactory<String> getEventSenderFactory(final int port) {
        final StringNettyEventSenderFactory senderFactory = new StringNettyEventSenderFactory(log,
                ADDRESS, port, TransportProtocol.TCP, CHARSET, LineEnding.UNIX);
        senderFactory.setTimeout(DEFAULT_TIMEOUT);
        return senderFactory;
    }

    private NettyEventServerFactory getEventServerFactory(final int port, final BlockingQueue<ByteArrayMessage> messages) {
        final ByteArrayMessageNettyEventServerFactory factory = new ByteArrayMessageNettyEventServerFactory(log,
                ADDRESS, port, TransportProtocol.TCP, DELIMITER.getBytes(), MAX_FRAME_LENGTH, messages);
        factory.setWorkerThreads(SINGLE_THREAD);
        return factory;
    }

    private SSLContext getSslContext() throws GeneralSecurityException, IOException {
        final TlsConfiguration tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();
        final File keystoreFile = new File(tlsConfiguration.getKeystorePath());
        keystoreFile.deleteOnExit();
        final File truststoreFile = new File(tlsConfiguration.getTruststorePath());
        truststoreFile.deleteOnExit();
        return SslContextFactory.createSslContext(tlsConfiguration);
    }
}
