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
package org.apache.nifi.minifi.bootstrap.service;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.util.LimitingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiNiFiListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiNiFiListener.class);

    private Listener listener;
    private ServerSocket serverSocket;

    public int start(RunMiNiFi runner) throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress("localhost", 0));

        listener = new Listener(serverSocket, runner);
        Thread listenThread = new Thread(listener);
        listenThread.setName("MiNiFi listener");
        listenThread.setDaemon(true);
        listenThread.start();
        return serverSocket.getLocalPort();
    }

    public void stop() {
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOGGER.error("Failed to close socket");
        }
        Optional.ofNullable(listener).ifPresent(Listener::stop);
    }

    private static class Listener implements Runnable {

        private final ServerSocket serverSocket;
        private final ExecutorService executor;
        private final RunMiNiFi runner;
        private volatile boolean stopped = false;

        public Listener(ServerSocket serverSocket, RunMiNiFi runner) {
            this.serverSocket = serverSocket;
            this.executor = Executors.newFixedThreadPool(2, runnable -> {
                Thread t = Executors.defaultThreadFactory().newThread(runnable);
                t.setDaemon(true);
                t.setName("MiNiFi Bootstrap Command Listener");
                return t;
            });

            this.runner = runner;
        }

        public void stop() {
            stopped = true;

            try {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    LOGGER.warn("Failed to stop the MiNiFi listener executor", e);
                    executor.shutdownNow();
                }

                serverSocket.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close socket", e);
            } catch (Exception e) {
                LOGGER.warn("Failed to stop the MiNiFi listener executor", e);
            }
        }

        @Override
        public void run() {
            while (!serverSocket.isClosed()) {
                try {
                    if (stopped) {
                        return;
                    }

                    Socket socket;
                    try {
                        socket = serverSocket.accept();
                    } catch (IOException ioe) {
                        if (stopped) {
                            return;
                        }
                        throw ioe;
                    }

                    executor.submit(() -> {
                        // we want to ensure that we don't try to read data from an InputStream directly
                        // by a BufferedReader because any user on the system could open a socket and send
                        // a multi-gigabyte file without any new lines in order to crash the Bootstrap,
                        // which in turn may cause the Shutdown Hook to shutdown MiNiFi.
                        // So we will limit the amount of data to read to 4 KB
                        try (InputStream limitingIn = new LimitingInputStream(socket.getInputStream(), 4096)) {
                            BootstrapCodec codec = new BootstrapCodec(runner, limitingIn, socket.getOutputStream());
                            codec.communicate();
                        } catch (Exception t) {
                            LOGGER.error("Failed to communicate with MiNiFi due to exception: ", t);
                        } finally {
                            try {
                                socket.close();
                            } catch (IOException ioe) {
                                LOGGER.warn("Failed to close the socket ", ioe);
                            }
                        }
                    });
                } catch (Exception t) {
                    LOGGER.error("Failed to receive information from MiNiFi due to exception: ", t);
                }
            }
        }
    }
}
