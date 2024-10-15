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

import org.apache.nifi.event.transport.EventDroppedException;
import org.apache.nifi.event.transport.EventException;
import org.apache.nifi.event.transport.EventSender;
import org.apache.nifi.event.transport.EventServer;
import org.apache.nifi.event.transport.configuration.LineEnding;
import org.apache.nifi.event.transport.configuration.ShutdownQuietPeriod;
import org.apache.nifi.event.transport.configuration.ShutdownTimeout;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.util.ClientAuth;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import javax.security.auth.x500.X500Principal;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class StringNettyEventSenderFactoryTest {
    private static final InetAddress ADDRESS;

    private static final int MAX_FRAME_LENGTH = 1024;

    private static final long TIMEOUT_SECONDS = 5;

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(TIMEOUT_SECONDS);

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final String MESSAGE = String.class.getName();

    private static final String DELIMITER = "\n";

    private static final int SINGLE_THREAD = 1;

    private static final int SINGLE_MESSAGE = 1;

    static {
        try {
            ADDRESS = InetAddress.getByName("127.0.0.1");
        } catch (final UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Captor
    private ArgumentCaptor<Exception> exceptionCaptor;

    @Mock
    private ComponentLog log;

    @Test
    public void testSendEventTcpEventException() throws Exception {
        final BlockingQueue<ByteArrayMessage> messages = new LinkedBlockingQueue<>();
        final NettyEventServerFactory serverFactory = getEventServerFactory(0, messages);
        final EventServer eventServer = serverFactory.getEventServer();
        final NettyEventSenderFactory<String> senderFactory = getEventSenderFactory(eventServer.getListeningPort());
        try (final EventSender<String> sender = senderFactory.getEventSender()) {
            eventServer.shutdown();
            assertThrows(EventException.class, () -> sender.sendEvent(MESSAGE));
        } finally {
            eventServer.shutdown();
        }
    }

    @Test
    public void testSendEventTcp() throws Exception {
        final BlockingQueue<ByteArrayMessage> messages = new LinkedBlockingQueue<>();
        final NettyEventServerFactory serverFactory = getEventServerFactory(0, messages);
        final EventServer eventServer = serverFactory.getEventServer();
        final NettyEventSenderFactory<String> senderFactory = getEventSenderFactory(eventServer.getListeningPort());
        try (final EventSender<String> sender = senderFactory.getEventSender()) {
            sender.sendEvent(MESSAGE);
        } finally {
            eventServer.shutdown();
        }

        assertMessageReceived(messages);
    }

    @Test
    public void testSendEventTcpEventDropped() throws Exception {
        final BlockingQueue<ByteArrayMessage> messages = new LinkedBlockingQueue<>(SINGLE_MESSAGE);
        final NettyEventServerFactory serverFactory = getEventServerFactory(0, messages);
        final EventServer eventServer = serverFactory.getEventServer();
        final NettyEventSenderFactory<String> senderFactory = getEventSenderFactory(eventServer.getListeningPort());
        try (final EventSender<String> sender = senderFactory.getEventSender()) {
            sender.sendEvent(MESSAGE);
            assertMessageMatched(messages.take());

            // Send event to be queued
            sender.sendEvent(MESSAGE);

            // Send event to be dropped
            sender.sendEvent(MESSAGE);
        } finally {
            eventServer.shutdown();
        }

        assertEquals(SINGLE_MESSAGE, messages.size());

        verify(log).warn(anyString(), any(SocketAddress.class), exceptionCaptor.capture());
        final Exception exception = exceptionCaptor.getValue();
        assertInstanceOf(EventDroppedException.class, exception);
    }

    @Test
    public void testSendEventTcpSslContextConfigured() throws Exception {
        final SSLContext sslContext = getSslContext();

        final BlockingQueue<ByteArrayMessage> messages = new LinkedBlockingQueue<>();
        final NettyEventServerFactory serverFactory = getEventServerFactory(0, messages);
        serverFactory.setSslContext(sslContext);
        serverFactory.setClientAuth(ClientAuth.NONE);
        final EventServer eventServer = serverFactory.getEventServer();

        final NettyEventSenderFactory<String> senderFactory = getEventSenderFactory(eventServer.getListeningPort());
        senderFactory.setSslContext(sslContext);

        try (final EventSender<String> eventSender = senderFactory.getEventSender()) {
            eventSender.sendEvent(MESSAGE);
        } finally {
            eventServer.shutdown();
        }

        assertMessageReceived(messages);
    }

    private void assertMessageReceived(final BlockingQueue<ByteArrayMessage> messages) throws InterruptedException {
        final ByteArrayMessage messageReceived = messages.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(messageReceived, "Message not received");
        assertMessageMatched(messageReceived);
    }

    private void assertMessageMatched(final ByteArrayMessage messageReceived) {
        final String eventReceived = new String(messageReceived.getMessage(), CHARSET);
        assertEquals(MESSAGE, eventReceived, "Message not matched");
        assertEquals(ADDRESS.getHostAddress(), messageReceived.getSender(), "Sender not matched");
    }

    private NettyEventSenderFactory<String> getEventSenderFactory(final int port) {
        final StringNettyEventSenderFactory senderFactory = new StringNettyEventSenderFactory(log,
                ADDRESS.getHostAddress(), port, TransportProtocol.TCP, CHARSET, LineEnding.UNIX);
        senderFactory.setTimeout(DEFAULT_TIMEOUT);
        senderFactory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        senderFactory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        return senderFactory;
    }

    private NettyEventServerFactory getEventServerFactory(final int port, final BlockingQueue<ByteArrayMessage> messages) {
        final ByteArrayMessageNettyEventServerFactory factory = new ByteArrayMessageNettyEventServerFactory(log,
                ADDRESS, port, TransportProtocol.TCP, DELIMITER.getBytes(), MAX_FRAME_LENGTH, messages);
        factory.setWorkerThreads(SINGLE_THREAD);
        factory.setShutdownQuietPeriod(ShutdownQuietPeriod.QUICK.getDuration());
        factory.setShutdownTimeout(ShutdownTimeout.QUICK.getDuration());
        return factory;
    }

    private SSLContext getSslContext() throws GeneralSecurityException {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();
        return new StandardSslContextBuilder()
                .trustStore(keyStore)
                .keyStore(keyStore)
                .keyPassword(new char[]{})
                .build();
    }
}
