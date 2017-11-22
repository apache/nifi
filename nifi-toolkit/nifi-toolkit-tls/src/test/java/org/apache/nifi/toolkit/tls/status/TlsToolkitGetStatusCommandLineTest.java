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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.net.URI;

import static org.junit.Assert.fail;

public class TlsToolkitGetStatusCommandLineTest {

    private TlsToolkitGetStatusCommandLine commandLine;

    @Before
    public void setup() {
        commandLine = new TlsToolkitGetStatusCommandLine();
    }

    @Test
    public void testHelp() {
        try {
            commandLine.parse("-h");
            fail("Expected usage and help exit");
        } catch (CommandLineParseException e) {
            Assert.assertEquals(ExitCode.HELP, e.getExitCode());
        }
    }

    @Test
    public void testSuccess() {
        try {
            final String urlStr = "https://localhost:8443/test";
            commandLine.parse(
                    "-u", urlStr,
                    "-ts", "src/test/resources/localhost/truststore.jks",
                    "-tst", "JKS",
                    "-tsp", "t7rmn1fg8np2ck1sduqdd85opv");

            final GetStatusConfig config = commandLine.createConfig();
            Assert.assertNotNull(config);

            final URI url = config.getUrl();
            Assert.assertNotNull(url);
            Assert.assertEquals(urlStr, url.toString());

            final SSLContext sslContext = config.getSslContext();
            Assert.assertNotNull(sslContext);
        } catch (CommandLineParseException e) {
            fail("Expected success");
        }
    }

    @Test
    public void testMissingUrl() {
        try {
            commandLine.parse(
                    "-ts", "src/test/resources/localhost/truststore.jks",
                    "-tst", "JKS",
                    "-tsp", "t7rmn1fg8np2ck1sduqdd85opv");

            fail("Expected invalid args");
        } catch (CommandLineParseException e) {
            Assert.assertEquals(ExitCode.INVALID_ARGS, e.getExitCode());
        }
    }

    @Test
    public void testTruststoreDoesNotExist() {
        try {
            final String urlStr = "https://localhost:8443/test";
            commandLine.parse(
                    "-u", urlStr,
                    "-ts", "does/not/exist/truststore.jks",
                    "-tst", "JKS",
                    "-tsp", "t7rmn1fg8np2ck1sduqdd85opv");

            fail("Expected invalid args");
        } catch (CommandLineParseException e) {
            Assert.assertEquals(ExitCode.INVALID_ARGS, e.getExitCode());
        }
    }

    @Test
    public void testInvalidTruststoreType() {
        try {
            final String urlStr = "https://localhost:8443/test";
            commandLine.parse(
                    "-u", urlStr,
                    "-ts", "src/test/resources/localhost/truststore.jks",
                    "-tst", "INVALID",
                    "-tsp", "t7rmn1fg8np2ck1sduqdd85opv");

            fail("Expected invalid args");
        } catch (CommandLineParseException e) {
            Assert.assertEquals(ExitCode.INVALID_ARGS, e.getExitCode());
        }
    }

}
