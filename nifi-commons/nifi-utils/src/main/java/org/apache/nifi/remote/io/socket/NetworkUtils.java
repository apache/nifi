/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
*/
package org.apache.nifi.remote.io.socket;

import java.net.DatagramSocket;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class NetworkUtils {

    /**
     * Get Available TCP Port
     *
     * @return Available TCP Port
     */
    public static int availablePort() {
        return getAvailableTcpPort();
    }

    /**
     * Get Available TCP Port using ServerSocket
     *
     * @return Available TCP Port
     */
    public static int getAvailableTcpPort() {
        try (final ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (final Exception e) {
            throw new IllegalArgumentException("Available TCP Port not found", e);
        }
    }

    /**
     * Get Available UDP Port using DatagramSocket
     *
     * @return Available UDP Port
     */
    public static int getAvailableUdpPort() {
        try (final DatagramSocket socket = new DatagramSocket()) {
            return socket.getLocalPort();
        } catch (final Exception e) {
            throw new IllegalArgumentException("Available UDP Port not found", e);
        }
    }

    public static boolean isListening(final String hostname, final int port) {
        try (final Socket s = new Socket(hostname, port)) {
            return s.isConnected();
        } catch (final Exception ignore) {}
        return false;
    }

    public static boolean isListening(final String hostname, final int port, final int timeoutMillis) {
        Boolean result = false;

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            result = executor.submit(() -> {
                while(!isListening(hostname, port)) {
                    try {
                        Thread.sleep(100);
                    } catch (final Exception ignore) {}
                }
                return true;
            }).get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final Exception ignore) {} finally {
            try {
                executor.shutdown();
            } catch (final Exception ignore) {}
        }

        return (result != null && result);
    }
}
