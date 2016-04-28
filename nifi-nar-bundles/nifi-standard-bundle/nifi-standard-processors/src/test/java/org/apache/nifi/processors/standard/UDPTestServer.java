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
package org.apache.nifi.processors.standard;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.concurrent.ArrayBlockingQueue;

public class UDPTestServer implements Runnable {

    private final int MAX_DATAGRAM_PACKET_SIZE = 1000000;
    private final InetAddress ipAddress;
    private final int port;
    private volatile DatagramSocket serverSocket;
    private final ArrayBlockingQueue<DatagramPacket> recvQueue;

    public UDPTestServer(final InetAddress ipAddress, final int port, final ArrayBlockingQueue<DatagramPacket> recvQueue) {
        this.ipAddress = ipAddress;
        this.port = port;
        this.recvQueue = recvQueue;
    }

    public synchronized void startServer() throws SocketException {
        if (!isRunning()) {
            serverSocket = new DatagramSocket(port, ipAddress);
            Thread t = new Thread(this);
            t.setName(this.getClass().getSimpleName());
            t.start();
        }
    }

    public synchronized void shutdownServer() {
        if (isRunning()) {
            serverSocket.close();
            serverSocket = null;
        }
    }

    private DatagramPacket createDatagramPacket() {
        return new DatagramPacket(new byte[MAX_DATAGRAM_PACKET_SIZE], MAX_DATAGRAM_PACKET_SIZE);
    }

    private void storeReceivedPacket(final DatagramPacket packet) {
        recvQueue.add(packet);
    }

    private boolean isRunning() {
        return serverSocket != null && !serverSocket.isClosed();
    }

    public DatagramPacket getReceivedPacket() {
        return recvQueue.poll();
    }

    public int getLocalPort() {
        return serverSocket == null ? 0 : serverSocket.getLocalPort();
    }

    @Override
    public void run() {
        try {
            while (isRunning()) {
                DatagramPacket packet = createDatagramPacket();
                serverSocket.receive(packet);
                storeReceivedPacket(packet);
            }
        } catch (Exception e) {
            // Do Nothing
        } finally {
            shutdownServer();
        }

    }
}
