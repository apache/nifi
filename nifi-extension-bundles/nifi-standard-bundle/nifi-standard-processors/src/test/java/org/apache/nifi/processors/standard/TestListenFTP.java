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

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestListenFTP {

    private static final String LOCALHOST = "127.0.0.1";

    private static final Pattern PASSIVE_REPLY_PATTERN = Pattern.compile("\\((\\d+),(\\d+),(\\d+),(\\d+),(\\d+),(\\d+)\\)");

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(ListenFTP.class);
    }

    @AfterEach
    void shutdownRunner() {
        runner.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {"50000-50099", "50000-50000", "1-65535", "${PASSIVE_PORT_RANGE}"})
    void testPassivePortRangeValid(final String passivePortRange) {
        runner.setProperty(ListenFTP.PASSIVE_PORT_RANGE, passivePortRange);

        runner.assertValid();
    }

    @ParameterizedTest
    @ValueSource(strings = {"50000", "50099-50000", "0-100", "50000-65536", "50000-50099,50200", "start-end", " "})
    void testPassivePortRangeInvalid(final String passivePortRange) {
        runner.setProperty(ListenFTP.PASSIVE_PORT_RANGE, passivePortRange);

        runner.assertNotValid();
    }

    @Test
    void testPassiveModeUsesConfiguredPortRange() throws IOException {
        final int port = getAvailablePort();
        final int passivePort = getAvailablePort();
        runner.setProperty(ListenFTP.ADDRESS, LOCALHOST);
        runner.setProperty(ListenFTP.PORT, Integer.toString(port));
        runner.setProperty(ListenFTP.PASSIVE_PORT_RANGE, passivePort + "-" + passivePort);

        runner.run(1, false);

        final FTPClient client = new FTPClient();
        try {
            client.connect(LOCALHOST, port);
            assertTrue(client.login("anonymous", ""));

            assertEquals(FTPReply.ENTERING_PASSIVE_MODE, client.pasv());
            assertEquals(passivePort, getPassivePort(client.getReplyString()));
        } finally {
            client.disconnect();
            runner.stop();
        }
    }

    private int getPassivePort(final String passiveReply) {
        final Matcher matcher = PASSIVE_REPLY_PATTERN.matcher(passiveReply);
        assertTrue(matcher.find(), "Unexpected passive mode reply: " + passiveReply);
        return Integer.parseInt(matcher.group(5)) * 256 + Integer.parseInt(matcher.group(6));
    }

    private int getAvailablePort() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }
}
