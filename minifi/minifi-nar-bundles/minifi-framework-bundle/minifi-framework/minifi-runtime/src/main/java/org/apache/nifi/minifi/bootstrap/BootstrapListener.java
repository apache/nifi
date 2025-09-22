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

package org.apache.nifi.minifi.bootstrap;

import static org.apache.nifi.bootstrap.CommandResult.FAILURE;
import static org.apache.nifi.bootstrap.CommandResult.SUCCESS;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import org.apache.nifi.bootstrap.BootstrapCommunicator;
import org.apache.nifi.bootstrap.CommandResult;
import org.apache.nifi.minifi.MiNiFiServer;
import org.apache.nifi.minifi.commons.status.FlowStatusReport;
import org.apache.nifi.minifi.status.StatusRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootstrapListener implements BootstrapCommunicator {

    private static final Logger logger = LoggerFactory.getLogger(BootstrapListener.class);
    private static final String RELOAD = "RELOAD";
    private static final String SHUTDOWN = "SHUTDOWN";
    private static final String STARTED = "STARTED";
    private static final int LISTENER_EXECUTOR_THREAD_COUNT = 2;

    private final MiNiFiServer minifiServer;
    private final BootstrapRequestReader bootstrapRequestReader;

    private final int bootstrapPort;
    private final String secretKey;
    private final ObjectMapper objectMapper;

    private Listener listener;
    private final Map<String, BiConsumer<String[], OutputStream>> messageHandlers = new HashMap<>();

    public BootstrapListener(MiNiFiServer minifiServer, int bootstrapPort) {
        this.minifiServer = minifiServer;
        this.bootstrapPort = bootstrapPort;
        secretKey = UUID.randomUUID().toString();
        bootstrapRequestReader = new BootstrapRequestReader(secretKey);

        objectMapper = new ObjectMapper();
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        registerHandlers();
    }

    public void start(int listenPort) throws IOException {
        logger.debug("Starting Bootstrap Listener to communicate with Bootstrap Port {}", bootstrapPort);

        ServerSocket serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress("localhost", listenPort));
        serverSocket.setSoTimeout(2000);

        int localPort = serverSocket.getLocalPort();
        logger.info("Started Bootstrap Listener, Listening for incoming requests on port {}", localPort);

        listener = new Listener(serverSocket);
        Thread listenThread = new Thread(listener);
        listenThread.setDaemon(true);
        listenThread.setName("Listen to Bootstrap");
        listenThread.start();

        logger.debug("Notifying Bootstrap that local port is {}", localPort);
        sendCommand("PORT", new String[] {String.valueOf(localPort), secretKey});
    }

    public void reload() throws IOException {
        if (listener != null) {
            listener.stop();
        }
        sendCommand(RELOAD, new String[] {});
    }

    public void stop() throws IOException {
        if (listener != null) {
            listener.stop();
        }
        sendCommand(SHUTDOWN, new String[] {});
    }

    public void sendStartedStatus(boolean status) throws IOException {
        logger.debug("Notifying Bootstrap that the status of starting MiNiFi is {}", status);
        sendCommand(STARTED, new String[] {String.valueOf(status)});
    }

    @Override
    public CommandResult sendCommand(String command, String[] args) throws IOException {
        try (Socket socket = new Socket()) {
            socket.setSoTimeout(60000);
            socket.connect(new InetSocketAddress("localhost", bootstrapPort));

            StringBuilder commandBuilder = new StringBuilder(command);

            Arrays.stream(args).forEach(arg -> commandBuilder.append(" ").append(arg));
            commandBuilder.append("\n");

            String commandWithArgs = commandBuilder.toString();
            logger.debug("Sending command to Bootstrap: {}", commandWithArgs);

            OutputStream out = socket.getOutputStream();
            out.write((commandWithArgs).getBytes(StandardCharsets.UTF_8));
            out.flush();

            logger.debug("Awaiting response from Bootstrap...");
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String response = reader.readLine();
            if ("OK".equals(response)) {
                logger.info("Successfully initiated communication with Bootstrap");
                return SUCCESS;
            } else {
                logger.error("Failed to communicate with Bootstrap. Bootstrap may be unable to issue or receive commands from MiNiFi");
                return FAILURE;
            }
        }
    }

    @Override
    public void registerMessageHandler(String command, BiConsumer<String[], OutputStream> handler) {
        messageHandlers.putIfAbsent(command, handler);
    }

    private void registerHandlers() {
        messageHandlers.putIfAbsent("PING", (args, outputStream) -> {
            logger.debug("Received PING request from Bootstrap; responding");
            echoRequestCmd("PING", outputStream);
            logger.debug("Responded to PING request from Bootstrap");
        });
        messageHandlers.putIfAbsent(RELOAD, (args, outputStream) -> {
            logger.info("Received RELOAD request from Bootstrap");
            echoRequestCmd(RELOAD, outputStream);
            logger.info("Responded to RELOAD request from Bootstrap, stopping MiNiFi Server");
            minifiServer.stop(true);
        });
        messageHandlers.putIfAbsent(SHUTDOWN, (args, outputStream) -> {
            logger.info("Received SHUTDOWN request from Bootstrap");
            echoRequestCmd(SHUTDOWN, outputStream);
            logger.info("Responded to SHUTDOWN request from Bootstrap, stopping MiNiFi Server");
            minifiServer.stop(false);
        });
        messageHandlers.putIfAbsent("DUMP", (args, outputStream) -> {
            logger.info("Received DUMP request from Bootstrap");
            writeDump(outputStream);
        });
        messageHandlers.putIfAbsent("FLOW_STATUS_REPORT", (args, outputStream) -> {
            logger.info("Received FLOW_STATUS_REPORT request from Bootstrap");
            String flowStatusRequestString = args[0];
            writeStatusReport(flowStatusRequestString, outputStream);
        });
        messageHandlers.putIfAbsent("ENV", (args, outputStream) -> {
            logger.info("Received ENV request from Bootstrap");
            writeEnv(outputStream);
        });
    }

    private class Listener implements Runnable {

        private final ServerSocket serverSocket;
        private final ExecutorService executor;
        private volatile boolean stopped = false;

        public Listener(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
            this.executor = Executors.newFixedThreadPool(LISTENER_EXECUTOR_THREAD_COUNT);
        }

        public void stop() {
            stopped = true;

            executor.shutdownNow();

            try {
                serverSocket.close();
            } catch (IOException ignored) {
                // nothing to really do here. we could log this, but it would just become
                // confusing in the logs, as we're shutting down and there's no real benefit
            }
        }

        @Override
        public void run() {
            while (!stopped) {
                try {
                    Socket socket;
                    try {
                        logger.debug("Listening for Bootstrap Requests");
                        socket = serverSocket.accept();
                    } catch (SocketTimeoutException ste) {
                        if (stopped) {
                            return;
                        }
                        continue;
                    } catch (IOException ioe) {
                        if (stopped) {
                            return;
                        }
                        throw ioe;
                    }

                    logger.debug("Received connection from Bootstrap");
                    socket.setSoTimeout(5000);

                    executor.submit(() -> handleBootstrapRequest(socket));
                } catch (Throwable t) {
                    logger.error("Failed to process request from Bootstrap", t);
                }
            }
        }

        private void handleBootstrapRequest(Socket socket) {
            try {
                BootstrapRequest request = bootstrapRequestReader.readRequest(socket.getInputStream());
                String requestType = request.getRequestType();

                BiConsumer<String[], OutputStream> handler = messageHandlers.get(requestType);
                if (handler == null) {
                    logger.warn("There is no handler defined for the {}", requestType);
                } else {
                    handler.accept(request.getArgs(), socket.getOutputStream());
                }

            } catch (Throwable t) {
                logger.error("Failed to process request from Bootstrap", t);
            } finally {
                try {
                    socket.close();
                } catch (IOException ioe) {
                    logger.warn("Failed to close socket to Bootstrap", ioe);
                }
            }
        }

    }

    private void writeStatusReport(String flowStatusRequestString, OutputStream out) throws StatusRequestException {
        try {
            FlowStatusReport flowStatusReport = minifiServer.getStatusReport(flowStatusRequestString);
            objectMapper.writeValue(out, flowStatusReport);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeEnv(OutputStream out) {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
            StringBuilder sb = new StringBuilder();

            System.getProperties()
                .forEach((key, value) -> sb.append(key).append("=").append(value).append("\n"));

            writer.write(sb.toString());
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeDump(OutputStream out) {
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
            writer.write(DumpUtil.getDump());
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void echoRequestCmd(String cmd, OutputStream out) {
        try {
            out.write((cmd + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            out.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
