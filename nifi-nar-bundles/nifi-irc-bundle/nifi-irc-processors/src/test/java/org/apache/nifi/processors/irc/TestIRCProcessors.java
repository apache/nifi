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
package org.apache.nifi.processors.irc;

import org.apache.nifi.irc.IRCClientService;
import org.apache.nifi.irc.StandardIRCClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class TestIRCProcessors {


    @Before
    public void init() {

    }

    @Ignore("Test requires network connectivity to an IRC server and left just for illustration purposes")
    @Test(timeout = 1200000L)
    public void testConsumeIRC() throws InitializationException, InterruptedException {
        // Setup the basics
        IRCClientService ircClientService = new StandardIRCClientService();


        // Setup first runner
        final TestRunner runner = TestRunners.newTestRunner(ConsumeIRC.class);
        runner.addControllerService("test", ircClientService);
        runner.setProperty(ircClientService, StandardIRCClientService.IRC_SERVER, "dev.lab");
        runner.setProperty(ircClientService, StandardIRCClientService.IRC_SERVER_PORT, "6667");
        runner.setProperty(ircClientService, StandardIRCClientService.IRC_NICK, "NiFi");
        runner.setProperty(ConsumeIRC.IRC_CLIENT_SERVICE, ircClientService.getIdentifier());
        runner.setProperty(ConsumeIRC.IRC_CHANNEL, "#nifi_");

        runner.enableControllerService(ircClientService);

        // Start the processor... and iterate once but let it run
        runner.run(1, false);



        List<MockFlowFile> results = new LinkedList<>();

        // loop waiting for content
        while (results.size() == 0) {
            results.addAll(runner.getFlowFilesForRelationship(ConsumeIRC.REL_SUCCESS));
        }
        Assert.assertTrue(results.size() >= 1);
        MockFlowFile response = results.get(0);
        response.assertContentEquals("...");

        runner.disableControllerService(ircClientService);
    }

    @Ignore("Test requires network connectivity to an IRC server and left just for illustration purposes")
    @Test(timeout = 1200000L)
    public void testPublishIRC() throws InitializationException, InterruptedException {
        // Setup the basics
        IRCClientService ircClientService = new StandardIRCClientService();

        // Setup first runner
        final TestRunner runner = TestRunners.newTestRunner(PublishIRC.class);
        runner.addControllerService("test", ircClientService);
        runner.setProperty(ircClientService, StandardIRCClientService.IRC_SERVER, "dev.lab");
        runner.setProperty(ircClientService, StandardIRCClientService.IRC_SERVER_PORT, "6667");
        runner.setProperty(ircClientService, StandardIRCClientService.IRC_NICK, "NiFi");

        runner.setProperty(PublishIRC.IRC_CLIENT_SERVICE, ircClientService.getIdentifier());
        runner.setProperty(PublishIRC.IRC_CHANNEL, "#nifi_");

        runner.enableControllerService(ircClientService);

        // Start the processor... and iterate once but let it run
        runner.run(1, false);

        List<MockFlowFile> results = new LinkedList<>();

        while (!ircClientService.getIsConnected().get()) {
            // wait for connection
        }

        runner.enqueue("test test test chocolate!");

        // Run a few times hoping all client setup will be complete by the end of the last iteration
        runner.run(20, false);

        // loop waiting for content
        while (results.size() == 0) {
            results.addAll(runner.getFlowFilesForRelationship(ConsumeIRC.REL_SUCCESS));
        }
        Assert.assertTrue(results.size() >= 1);

        runner.disableControllerService(ircClientService);
    }

    @Ignore("Test requires network connectivity to an IRC server and left just for illustration purposes")
    @Test(timeout = 1200000L)
    public void testPublishIRCWithTLS() throws InitializationException, InterruptedException {
        // Setup the basics
        IRCClientService ircClientService = new StandardIRCClientService();
        SSLContextService sslContextService = new StandardSSLContextService();


        // Setup first runner
        final TestRunner runner = TestRunners.newTestRunner(PublishIRC.class);

        runner.addControllerService("test", ircClientService);
        runner.setProperty(ircClientService, StandardIRCClientService.SSL_CONTEXT_SERVICE, "ssl-test");
        runner.setProperty(ircClientService, StandardIRCClientService.IRC_SERVER, "dev.lab");
        runner.setProperty(ircClientService, StandardIRCClientService.IRC_SERVER_PORT, "6697");
        runner.setProperty(ircClientService, StandardIRCClientService.IRC_NICK, "NiFi");
        // Set up the SSL context
        runner.addControllerService("ssl-test", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/localhost-ts.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/localhost-ks.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");



        runner.setProperty(PublishIRC.IRC_CLIENT_SERVICE, ircClientService.getIdentifier());
        runner.setProperty(PublishIRC.IRC_CHANNEL, "#nifi_");

        // Enable the SSL service followed by the IRC service
        runner.enableControllerService(sslContextService);
        runner.enableControllerService(ircClientService);

        // Start the processor... and iterate once but let it run
        runner.run(1, false);

        List<MockFlowFile> results = new LinkedList<>();

        while (!ircClientService.getIsConnected().get()) {
            // wait for connection
        }

        runner.enqueue("test test test chocolate!");

        // Run a few times hoping all client setup will be complete by the end of the last iteration
        runner.run(20, false);

        // loop waiting for content
        while (results.size() == 0) {
            results.addAll(runner.getFlowFilesForRelationship(ConsumeIRC.REL_SUCCESS));
        }
        Assert.assertTrue(results.size() >= 1);

        runner.disableControllerService(ircClientService);
    }



}
