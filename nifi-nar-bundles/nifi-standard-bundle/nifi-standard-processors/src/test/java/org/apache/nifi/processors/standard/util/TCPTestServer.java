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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.security.util.SslContextFactory;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class TCPTestServer implements Runnable {

    private final InetAddress ipAddress;
    private int port;
    private final String messageDelimiter;
    private volatile ServerSocket serverSocket;
    private final ArrayBlockingQueue<List<Byte>> recvQueue;
    private volatile Socket connectionSocket;
    public final static String DEFAULT_MESSAGE_DELIMITER = "\n";
    private volatile int totalNumConnections = 0;

    public TCPTestServer(final InetAddress ipAddress, final ArrayBlockingQueue<List<Byte>> recvQueue) {
        this(ipAddress, recvQueue, DEFAULT_MESSAGE_DELIMITER);
    }

    public TCPTestServer(final InetAddress ipAddress, final ArrayBlockingQueue<List<Byte>> recvQueue, final String messageDelimiter) {
        this.ipAddress = ipAddress;
        this.recvQueue = recvQueue;
        this.messageDelimiter = messageDelimiter;
    }

    public synchronized void startServer(boolean ssl) throws Exception {
        if (!isServerRunning()) {
            if(ssl){
                final SSLContext sslCtx = SslContextFactory.createSslContext("src/test/resources/keystore.jks","passwordpassword".toCharArray(), "JKS", "src/test/resources/truststore.jks",
                        "passwordpassword".toCharArray(), "JKS", SslContextFactory.ClientAuth.REQUIRED, "TLS");

                ServerSocketFactory sslSocketFactory = sslCtx.getServerSocketFactory();
                serverSocket = sslSocketFactory.createServerSocket(0, 0, ipAddress);
            } else {
                serverSocket = new ServerSocket(0, 0, ipAddress);
            }
            Thread t = new Thread(this);
            t.setName(this.getClass().getSimpleName());
            t.start();
            port = serverSocket.getLocalPort();
        }
    }

    public synchronized void shutdown() {
        shutdownConnection();
        shutdownServer();
    }

    public synchronized void shutdownServer() {
        if (isServerRunning()) {
            try {
                serverSocket.close();
            } catch (IOException ioe) {
                // Do Nothing.
            }
        }
    }

    public synchronized void shutdownConnection() {
        if (isConnected()) {
            try {
                connectionSocket.close();
            } catch (IOException ioe) {
                // Do Nothing.
            }
        }
    }

    public int getPort(){
        return port;
    }

    private void storeReceivedMessage(final List<Byte> message) {
        recvQueue.add(message);
    }

    private boolean isServerRunning() {
        return serverSocket != null && !serverSocket.isClosed();
    }

    private boolean isConnected() {
        return connectionSocket != null && !connectionSocket.isClosed();
    }

    public List<Byte> getReceivedMessage() {
        return recvQueue.poll();
    }

    public int getTotalNumConnections() {
        return totalNumConnections;
    }

    protected boolean isDelimiterPresent(final List<Byte> message) {
        if (messageDelimiter != null && message.size() >= messageDelimiter.length()) {
            for (int i = 1; i <= messageDelimiter.length(); i++) {
                if (message.get(message.size() - i).byteValue() == messageDelimiter.charAt(messageDelimiter.length() - i)) {
                    if (i == messageDelimiter.length()) {
                        return true;
                    }
                } else {
                    break;
                }
            }
        }
        return false;
    }

    protected boolean removeDelimiter(final List<Byte> message) {
        if (isDelimiterPresent(message)) {
            final int messageSize = message.size();
            for (int i = 1; i <= messageDelimiter.length(); i++) {
                message.remove(messageSize - i);
            }
            return true;
        }

        return false;
    }

    @Override
    public void run() {
        try {
            while (isServerRunning()) {
                connectionSocket = serverSocket.accept();
                totalNumConnections++;
                InputStream in = connectionSocket.getInputStream();
                while (isConnected()) {
                    final List<Byte> message = new ArrayList<Byte>();
                    while (true) {
                        final int c = in.read();
                        if (c < 0) {
                            if (!message.isEmpty()) {
                                storeReceivedMessage(message);
                            }
                            shutdownConnection();
                            break;
                        }

                        message.add((byte) c);

                        if (removeDelimiter(message)) {
                            storeReceivedMessage(message);
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Do Nothing
        } finally {
            shutdownConnection();
            shutdownServer();
        }

    }
}
