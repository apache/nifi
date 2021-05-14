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

import org.apache.nifi.io.socket.SocketUtils;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ServerSocketFactory;

public class TCPTestServer implements Runnable {

    private final InetAddress ipAddress;
    private final String messageDelimiter;
    private final ArrayBlockingQueue<List<Byte>> queue;
    private final AtomicInteger totalConnections = new AtomicInteger();
    private final boolean closeOnMessageReceived;

    private volatile ServerSocket serverSocket;
    private volatile Socket connectionSocket;
    private int port;

    public TCPTestServer(final InetAddress ipAddress, final ArrayBlockingQueue<List<Byte>> queue, final String messageDelimiter, final boolean closeOnMessageReceived) {
        this.ipAddress = ipAddress;
        this.queue = queue;
        this.messageDelimiter = messageDelimiter;
        this.closeOnMessageReceived = closeOnMessageReceived;
    }

    public synchronized void startServer(final ServerSocketFactory serverSocketFactory) throws Exception {
        if (!isServerRunning()) {
            if (serverSocketFactory == null) {
                serverSocket = new ServerSocket(0, 0, ipAddress);
            } else {
                serverSocket = serverSocketFactory.createServerSocket(0, 0, ipAddress);
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

    public int getPort(){
        return port;
    }

    private synchronized void shutdownServer() {
        if (isServerRunning()) {
            SocketUtils.closeQuietly(serverSocket);
        }
    }

    private synchronized void shutdownConnection() {
        if (isConnected()) {
            SocketUtils.closeQuietly(connectionSocket);
        }
    }

    private void storeReceivedMessage(final List<Byte> message) {
        queue.add(message);
        if (closeOnMessageReceived) {
            shutdownConnection();
        }
    }

    private boolean isServerRunning() {
        return serverSocket != null && !serverSocket.isClosed();
    }

    private boolean isConnected() {
        return connectionSocket != null && !connectionSocket.isClosed();
    }

    public int getTotalConnections() {
        return totalConnections.get();
    }

    protected boolean isDelimiterPresent(final List<Byte> message) {
        if (messageDelimiter != null && message.size() >= messageDelimiter.length()) {
            for (int i = 1; i <= messageDelimiter.length(); i++) {
                if (message.get(message.size() - i) == messageDelimiter.charAt(messageDelimiter.length() - i)) {
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
                totalConnections.incrementAndGet();
                final InputStream inputStream = connectionSocket.getInputStream();
                while (isConnected()) {
                    final List<Byte> message = new ArrayList<>();
                    while (true) {
                        final int c = inputStream.read();
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
            shutdown();
        }

    }
}
