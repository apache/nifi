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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
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
    private static final int SOCKET_TIMEOUT = 10000;
    private static final int CONNECTION_TIMEOUT = 10000;

    private final MiNiFiParameters miNiFiParameters;
    private final ObjectMapper objectMapper;

    public MiNiFiCommandSender(MiNiFiParameters miNiFiParameters, ObjectMapper objectMapper) {
        this.miNiFiParameters = miNiFiParameters;
        this.objectMapper = objectMapper;
    }

    public Optional<String> sendCommand(String cmd, Integer port, String... extraParams) throws IOException {
        Optional<String> response = Optional.empty();

        if (port == null) {
            LOGGER.info("Apache MiNiFi is not currently running");
            return response;
        }

        try (Socket socket = new Socket()) {
            LOGGER.debug("Connecting to MiNiFi instance");
            socket.setSoTimeout(SOCKET_TIMEOUT);
            socket.connect(new InetSocketAddress("localhost", port), CONNECTION_TIMEOUT);
            LOGGER.debug("Established connection to MiNiFi instance.");

            LOGGER.debug("Sending {} Command to port {}", cmd, port);

            String responseString;
            try (OutputStream out = socket.getOutputStream()) {
                out.write(getCommand(cmd, extraParams));
                out.flush();
                responseString = readResponse(socket);
            }

            LOGGER.debug("Received response to {} command: {}", cmd, responseString);
            response = Optional.of(responseString);
        } catch (EOFException | SocketTimeoutException e) {
            String message = "Failed to get response for " + cmd + " Potentially due to the process currently being down (restarting or otherwise)";
            throw new RuntimeException(message);
        }
        return response;
    }

    <T> T sendCommandForObject(String cmd, Integer port, Class<T> clazz, String... extraParams) throws IOException {
        return sendCommand(cmd, port, extraParams)
            .map(response -> deserialize(cmd, response, clazz))
            .orElse(null);
    }

    private String readResponse(Socket socket) throws IOException {
        StringBuilder sb = new StringBuilder();
        int numLines = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (numLines++ > 0) {
                    sb.append("\n");
                }
                sb.append(line);
            }
        }

        return sb.toString().trim();
    }

    private byte[] getCommand(String cmd, String... args) {
        String argsString = Arrays.stream(args).collect(Collectors.joining(" "));
        String commandWithArgs = cmd + " " + miNiFiParameters.getSecretKey() + (args.length > 0 ? " " : "") + argsString + "\n";
        return commandWithArgs.getBytes(StandardCharsets.UTF_8);
    }

    private <T> T deserialize(String cmd, String obj, Class<T> clazz) {
        T response;
        try {
            response = objectMapper.readValue(obj, clazz);
        } catch (JsonProcessingException e) {
            String message = "Failed to deserialize " + cmd + " response";
            LOGGER.error(message);
            throw new RuntimeException(message, e);
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
