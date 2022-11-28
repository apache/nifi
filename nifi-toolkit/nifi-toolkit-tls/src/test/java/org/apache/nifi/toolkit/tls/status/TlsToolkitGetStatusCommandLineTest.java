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
package org.apache.nifi.toolkit.tls.status;

import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.configuration.GetStatusConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TlsToolkitGetStatusCommandLineTest {

    private final String TRUSTSTORE_PATH = "src/test/resources/localhost/truststore.jks";
    private final String TRUSTSTORE_PASSWORD = "passwordpassword";
    private final String JKS_TYPE = "JKS";

    private TlsToolkitGetStatusCommandLine commandLine;

    @BeforeEach
    public void setup() {
        commandLine = new TlsToolkitGetStatusCommandLine();
    }

    @Test
    public void testHelp() {
        final CommandLineParseException e = assertThrows(CommandLineParseException.class, () -> commandLine.parse("-h"));

        assertEquals(ExitCode.HELP, e.getExitCode());
    }

    @Test
    public void testSuccess() throws CommandLineParseException {
        final String urlStr = "https://localhost:8443/test";
        commandLine.parse(
                "-u", urlStr,
                "-ts", TRUSTSTORE_PATH,
                "-tst", JKS_TYPE,
                "-tsp", TRUSTSTORE_PASSWORD);

        final GetStatusConfig config = commandLine.createConfig();
        assertNotNull(config);

        final URI url = config.getUrl();
        assertNotNull(url);
        assertEquals(urlStr, url.toString());

        final SSLContext sslContext = config.getSslContext();
        assertNotNull(sslContext);
    }

    @Test
    public void testMissingUrl() {
        final CommandLineParseException e = assertThrows(CommandLineParseException.class, () -> commandLine.parse(
                "-ts", TRUSTSTORE_PATH,
                "-tst", JKS_TYPE,
                "-tsp", TRUSTSTORE_PASSWORD)
        );

        assertEquals(ExitCode.INVALID_ARGS, e.getExitCode());
    }

    @Test
    public void testTruststoreDoesNotExist() {
        final CommandLineParseException e = assertThrows(CommandLineParseException.class, () -> commandLine.parse(

                "-u", "https://localhost:8443/test",
                "-ts", "does/not/exist/truststore.jks",
                "-tst", JKS_TYPE,
                "-tsp", TRUSTSTORE_PASSWORD)
        );

        assertEquals(ExitCode.INVALID_ARGS, e.getExitCode());
    }

    @Test
    public void testInvalidTruststoreType() {
        final CommandLineParseException e = assertThrows(CommandLineParseException.class, () -> commandLine.parse(

                "-u", "https://localhost:8443/test",
                "-ts", TRUSTSTORE_PATH,
                "-tst", "INVALID",
                "-tsp", TRUSTSTORE_PASSWORD)
        );

        assertEquals(ExitCode.INVALID_ARGS, e.getExitCode());
    }
}
