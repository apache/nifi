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

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.nifi.minifi.bootstrap.MiNiFiParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiNiFiCommandSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(MiNiFiCommandSender.class);
    private static final String PING_CMD = "PING";

    private final MiNiFiParameters miNiFiParameters;

    public MiNiFiCommandSender(MiNiFiParameters miNiFiParameters) {
        this.miNiFiParameters = miNiFiParameters;
    }

    public Optional<String> sendCommand(String cmd, Integer port) throws IOException {
        Optional<String> response = Optional.empty();

        if (port == null) {
            LOGGER.info("Apache MiNiFi is not currently running");
            return response;
        }

        try (Socket socket = new Socket()) {
            LOGGER.debug("Connecting to MiNiFi instance");
            socket.setSoTimeout(10000);
            socket.connect(new InetSocketAddress("localhost", port));
            LOGGER.debug("Established connection to MiNiFi instance.");

            LOGGER.debug("Sending {} Command to port {}", cmd, port);
            OutputStream out = socket.getOutputStream();
            out.write((cmd + " " + miNiFiParameters.getSecretKey() + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            socket.shutdownOutput();

            InputStream in = socket.getInputStream();
            StringBuilder sb = new StringBuilder();
            int numLines = 0;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (numLines++ > 0) {
                        sb.append("\n");
                    }
                    sb.append(line);
                }
            }

            String responseString = sb.toString().trim();

            LOGGER.debug("Received response to {} command: {}", cmd, responseString);
            response = Optional.of(responseString);
        }
        return response;
    }

    <T> T sendCommandForObject(String cmd, Integer port, String... extraParams) throws IOException {
        T response;
        try (Socket socket = new Socket("localhost", port)) {
            OutputStream out = socket.getOutputStream();
            String argsString = Arrays.stream(extraParams).collect(Collectors.joining(" ", " ", ""));
            String commandWithArgs = cmd + " " + miNiFiParameters.getSecretKey() + argsString + "\n";
            out.write((commandWithArgs).getBytes(StandardCharsets.UTF_8));
            LOGGER.debug("Sending {} command to MiNiFi with the following args: [{}]", cmd, argsString);
            out.flush();

            socket.setSoTimeout(5000);
            InputStream in = socket.getInputStream();

            ObjectInputStream ois = new ObjectInputStream(in);
            Object o = ois.readObject();
            ois.close();
            out.close();
            response = castResponse(cmd, o);
        } catch (EOFException | ClassNotFoundException | SocketTimeoutException e) {
            String message = "Failed to get response for " + cmd + " Potentially due to the process currently being down (restarting or otherwise). Exception:" + e;
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        return response;
    }

    private <T> T castResponse(String cmd, Object o) {
        T response;
        try {
            response = (T) o;
        } catch (ClassCastException e) {
            String message = "Failed to cast " + cmd + " response to the requested type";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        return response;
    }

    public boolean isPingSuccessful(int port) {
        try {
            return sendCommand(PING_CMD, port).filter(PING_CMD::equals).isPresent();
        } catch (IOException ioe) {
            return false;
        }
    }
}
