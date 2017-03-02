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
package org.apache.nifi.remote.io.socket.ssl;

//package nifi.remote.io.socket.ssl;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
//import java.io.ByteArrayOutputStream;
//import java.io.DataInputStream;
//import java.io.DataOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.net.Socket;
//import java.net.SocketTimeoutException;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.util.Arrays;
//import java.util.concurrent.TimeUnit;
//
//import javax.net.ssl.SSLContext;
//import javax.net.ssl.SSLServerSocket;
//import javax.net.ssl.SSLServerSocketFactory;
//
//import nifi.events.EventReporter;
//import nifi.framework.security.util.SslContextFactory;
//import nifi.groups.RemoteProcessGroup;
//import nifi.remote.RemoteGroupPort;
//import nifi.remote.RemoteResourceFactory;
//import nifi.remote.StandardSiteToSiteProtocol;
//import nifi.remote.TransferDirection;
//import nifi.remote.exception.HandshakeException;
//import nifi.remote.exception.PortNotRunningException;
//import nifi.remote.exception.UnknownPortException;
//import nifi.remote.protocol.CommunicationsProtocol;
//import nifi.remote.protocol.CommunicationsSession;
//import nifi.util.NiFiProperties;
//
//import org.junit.Assert;
//import org.junit.Ignore;
//import org.junit.Test;
//import org.mockito.Mockito;
//
//
////@Ignore("For local testing only")
//public class TestSSLSocketChannel {
//    public static final int DATA_SIZE = 4096;
//
//    @Test
//    @Ignore
//    public void testSendingToLocalInstance() throws IOException, InterruptedException, HandshakeException, UnknownPortException, PortNotRunningException, URISyntaxException {
//        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
//
//        final NiFiProperties properties;
//        final SSLContext sslContext = SslContextFactory.createSslContext(properties);
//
//        final SSLSocketChannel channel = new SSLSocketChannel(sslContext, "localhost", 5000, true);
//        channel.setTimeout(2000000);
//        channel.connect();
//
//        final CommunicationsSession commsSession;
//        commsSession = new SSLSocketChannelCommunicationsSession(channel, "", null);
//        commsSession.setUri("nifi://localhost:5000");
//        final DataInputStream dis = new DataInputStream(commsSession.getRequest().getInputStream());
//        final DataOutputStream dos = new DataOutputStream(commsSession.getResponse().getOutputStream());
//
//        dos.write(CommunicationsProtocol.MAGIC_BYTES);
//        dos.flush();
//
//        final EventReporter eventReporter = Mockito.mock(EventReporter.class);
//        final StandardSiteToSiteProtocol proposedProtocol = new StandardSiteToSiteProtocol(commsSession, eventReporter, nifiProperties);
//        final StandardSiteToSiteProtocol negotiatedProtocol = (StandardSiteToSiteProtocol) RemoteResourceFactory.initiateResourceNegotiation(proposedProtocol, dis, dos);
//        System.out.println(negotiatedProtocol);
//
//        final RemoteProcessGroup rpg = Mockito.mock(RemoteProcessGroup.class);
//        Mockito.when(rpg.getCommunicationsTimeout(Mockito.any(TimeUnit.class))).thenReturn(2000);
//        Mockito.when(rpg.getTargetUri()).thenReturn( new URI("https://localhost:5050/") );
//
//        final RemoteGroupPort port = Mockito.mock(RemoteGroupPort.class);
//        Mockito.when(port.getIdentifier()).thenReturn("90880680-d6da-40be-b2cc-a15423de2e1a");
//        Mockito.when(port.getName()).thenReturn("Data In");
//        Mockito.when(port.getRemoteProcessGroup()).thenReturn(rpg);
//
//        negotiatedProtocol.initiateHandshake(port, TransferDirection.SEND);
//    }
//
//    @Test
//    public void testWithSimpleSSLSocket() throws IOException, InterruptedException {
//        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
//
//        final NiFiProperties properties;
//        final SSLContext sslContext = SslContextFactory.createSslContext(properties);
//
//        final ServerThread server = new ServerThread(sslContext);
//        server.start();
//
//        int port = server.getPort();
//        while ( port <= 0 ) {
//            Thread.sleep(10L);
//            port = server.getPort();
//        }
//
//        final Socket socket = sslContext.getSocketFactory().createSocket("localhost", port);
//        final OutputStream out = socket.getOutputStream();
//
//        final byte[] sent = new byte[DATA_SIZE];
//        for (int i=0; i < sent.length; i++) {
//            sent[i] = (byte) (i % 255);
//        }
//
//        final long start = System.nanoTime();
//        out.write(sent);
//        final long nanos = System.nanoTime() - start;
//        final long millis = TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
//        final float seconds = (float) millis / 1000F;
//        final float megabytes = (float) DATA_SIZE / (1024F * 1024F);
//        final float MBperS = megabytes / seconds;
//        System.out.println("Millis: " + millis + "; MB/s: " + MBperS);
//    }
//
//    @Test
//    public void testDirectChannelComms() throws IOException, InterruptedException {
//        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
//
//        final NiFiProperties properties;
//        final SSLContext sslContext = SslContextFactory.createSslContext(properties);
//
//        final ServerThread server = new ServerThread(sslContext);
//        server.start();
//
//        int port = server.getPort();
//        while ( port <= 0 ) {
//            Thread.sleep(10L);
//            port = server.getPort();
//        }
//
//        final SSLSocketChannel channel = new SSLSocketChannel(sslContext, "localhost", port, true);
//        channel.setTimeout(1000);
//        channel.connect();
//
//        final byte[] sent = new byte[DATA_SIZE];
//        for (int i=0; i < sent.length; i++) {
//            sent[i] = (byte) (i % 255);
//        }
//
//        for (int itr=0; itr < 2; itr++) {
//            channel.write(sent);
//
//            Thread.sleep(250L);
//            final byte[] received = server.getReceivedData();
//            server.clearReceivedData();
//            assertTrue(Arrays.equals(sent, received));
//
//            int len;
//            final byte[] buffer = new byte[4096];
//            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            int bytesRead = 0;
//            while ((len = channel.read(buffer)) > 0 ) {
//                baos.write(buffer, 0, len);
//                bytesRead += len;
//                if ( bytesRead >= 8 ) {
//                    final byte[] receivedFromServer = baos.toByteArray();
//                    final long val = toLong(receivedFromServer);
//                    assertEquals(DATA_SIZE, val);
//                    System.out.println(val);
//                    break;
//                }
//            }
//        }
//
//        channel.close();
//        server.shutdown();
//    }
//
//
//    @Test
//    public void testWriteTimesOut() throws IOException, InterruptedException {
//        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
//
//        final NiFiProperties properties;
//        final SSLContext sslContext = SslContextFactory.createSslContext(properties);
//
//        final ServerThread server = new ServerThread(sslContext);
//        server.delayReading(2000);
//        server.start();
//
//        int port = server.getPort();
//        while ( port <= 0 ) {
//            Thread.sleep(10L);
//            port = server.getPort();
//        }
//
//        final SSLSocketChannel channel = new SSLSocketChannel(sslContext, "localhost", port, true);
//        channel.setTimeout(1000);
//        channel.connect();
//
//        final OutputStream out = new SSLSocketChannelOutputStream(channel);
//
//        final byte[] sent = new byte[1024 * 1024];
//        for (int i=0; i < sent.length; i++) {
//            sent[i] = (byte) (i % 255);
//        }
//
//        try {
//            out.write(sent);
//            Assert.fail("Did not timeout");
//        } catch (final SocketTimeoutException e) {
//            // expected result
//        }
//
//        server.delayReading(0);
//
//        try {
//            channel.close();
//        } catch (final Exception e) {}
//
//        server.shutdown();
//    }
//
//
//    @Test
//    public void testInputOutputStreams() throws IOException, InterruptedException {
//        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/nifi.properties");
//
//        final NiFiProperties properties;
//        final SSLContext sslContext = SslContextFactory.createSslContext(properties);
//
//        final ServerThread server = new ServerThread(sslContext);
//        server.start();
//
//        int port = server.getPort();
//        while ( port <= 0 ) {
//            Thread.sleep(10L);
//            port = server.getPort();
//        }
//
//        final SSLSocketChannel channel = new SSLSocketChannel(sslContext, "localhost", port, true);
//        channel.setTimeout(2000);
//        channel.connect();
//
//        final OutputStream out = new SSLSocketChannelOutputStream(channel);
//        final InputStream in = new SSLSocketChannelInputStream(channel);
//        final DataInputStream dataIn = new DataInputStream(in);
//
//        final byte[] sent = new byte[DATA_SIZE];
//        for (int i=0; i < sent.length; i++) {
//            sent[i] = (byte) (i % 255);
//        }
//
//        for (int itr=0; itr < 5; itr++) {
//            final long start = System.nanoTime();
//            out.write(sent);
//            final long nanos = System.nanoTime() - start;
//            final long millis = TimeUnit.MILLISECONDS.convert(nanos, TimeUnit.NANOSECONDS);
//            final float seconds = (float) millis / 1000F;
//            final float megabytes = (float) DATA_SIZE / (1024F * 1024F);
//            final float MBperS = megabytes / seconds;
//            System.out.println("Millis: " + millis + "; MB/s: " + MBperS);
//
//            Thread.sleep(500L);
//            final byte[] received = server.getReceivedData();
//            System.out.println("Server received " + received.length + " bytes");
//            server.clearReceivedData();
//            assertTrue(Arrays.equals(sent, received));
//
//            final long val = dataIn.readLong();
//            assertEquals(DATA_SIZE, val);
//            System.out.println(val);
//        }
//
//        channel.close();
//        server.shutdown();
//    }
//
//    public final long toLong(final byte[] buffer) throws IOException {
//        return (((long)buffer[0] << 56) +
//                ((long)(buffer[1] & 255) << 48) +
//                ((long)(buffer[2] & 255) << 40) +
//                ((long)(buffer[3] & 255) << 32) +
//                ((long)(buffer[4] & 255) << 24) +
//                ((buffer[5] & 255) << 16) +
//                ((buffer[6] & 255) <<  8) +
//                ((buffer[7] & 255) <<  0));
//    }
//
//    private static class ServerThread extends Thread {
//        private final SSLContext sslContext;
//        private int listeningPort;
//        private final ByteArrayOutputStream received = new ByteArrayOutputStream();
//
//        private volatile int readingDelay = 0;
//        private volatile boolean shutdown = false;
//
//        public ServerThread(final SSLContext sslContext) {
//            this.sslContext = sslContext;
//        }
//
//        public int getPort() {
//            return listeningPort;
//        }
//
//        public byte[] getReceivedData() {
//            return received.toByteArray();
//        }
//
//        @Override
//        public void run() {
//            try {
//                final SSLServerSocketFactory sslServerSocketFactory = sslContext.getServerSocketFactory();
//                final SSLServerSocket serverSocket = (SSLServerSocket) sslServerSocketFactory.createServerSocket(0);
//                serverSocket.setNeedClientAuth(true);
//
//                this.listeningPort = serverSocket.getLocalPort();
//
//                final Socket socket = serverSocket.accept();
//                socket.setSoTimeout(250);
//                final InputStream stream = socket.getInputStream();
//                final DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
//
//                final byte[] buffer = new byte[1024];
//                int len;
//
//                while (!shutdown) {
//                    try {
//                        len = stream.read(buffer);
//
//                        if ( readingDelay > 0 ) {
//                            try { Thread.sleep(readingDelay); } catch (final InterruptedException e) {}
//                        }
//                    } catch (final SocketTimeoutException e) {
//                        continue;
//                    }
//
//                    if ( len < 0 ) {
//                        return;
//                    }
//
//                    received.write(buffer, 0, len);
//
//                    final long length = received.size();
//                    if ( length % (DATA_SIZE) == 0 ) {
//                        dos.writeLong(length);
//                        dos.flush();
//                    }
//                }
//
//                System.out.println("Server successfully shutdown");
//            } catch (final Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        public void clearReceivedData() {
//            this.received.reset();
//        }
//
//        public void shutdown() {
//            this.shutdown = true;
//        }
//
//        public void delayReading(final int millis) {
//            this.readingDelay = millis;
//        }
//    }
//}
