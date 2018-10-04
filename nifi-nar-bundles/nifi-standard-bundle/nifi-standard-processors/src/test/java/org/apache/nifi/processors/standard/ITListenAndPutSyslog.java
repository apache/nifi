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

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Tests PutSyslog sending messages to ListenSyslog to simulate a syslog server forwarding
 * to ListenSyslog, or PutSyslog sending to a syslog server.
 */
public class ITListenAndPutSyslog {

    static final Logger LOGGER = LoggerFactory.getLogger(ITListenAndPutSyslog.class);

    private ListenSyslog listenSyslog;
    private TestRunner listenSyslogRunner;

    private PutSyslog putSyslog;
    private TestRunner putSyslogRunner;

    @Before
    public void setup() {
        this.listenSyslog = new ListenSyslog();
        this.listenSyslogRunner = TestRunners.newTestRunner(listenSyslog);

        this.putSyslog = new PutSyslog();
        this.putSyslogRunner = TestRunners.newTestRunner(putSyslog);
    }

    @After
    public void teardown() {
        try {
            putSyslog.onStopped();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        try {
            listenSyslog.onUnscheduled();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Test
    public void testUDP() throws IOException, InterruptedException {
        run(ListenSyslog.UDP_VALUE.getValue(), 5, 5);
    }

    @Test
    public void testTCP() throws IOException, InterruptedException {
        run(ListenSyslog.TCP_VALUE.getValue(), 5, 5);
    }

    @Test
    public void testTLS() throws InitializationException, IOException, InterruptedException {
        configureSSLContextService(listenSyslogRunner);
        listenSyslogRunner.setProperty(ListenSyslog.SSL_CONTEXT_SERVICE, "ssl-context");

        configureSSLContextService(putSyslogRunner);
        putSyslogRunner.setProperty(PutSyslog.SSL_CONTEXT_SERVICE, "ssl-context");

        run(ListenSyslog.TCP_VALUE.getValue(), 7, 7);
    }

    @Test
    public void testTLSListenerNoTLSPut() throws InitializationException, IOException, InterruptedException {
        configureSSLContextService(listenSyslogRunner);
        listenSyslogRunner.setProperty(ListenSyslog.SSL_CONTEXT_SERVICE, "ssl-context");

        // send 7 but expect 0 because sender didn't use TLS
        run(ListenSyslog.TCP_VALUE.getValue(), 7, 0);
    }

    private SSLContextService configureSSLContextService(TestRunner runner) throws InitializationException {
        final SSLContextService sslContextService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/truststore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/keystore.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "passwordpassword");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");
        runner.enableControllerService(sslContextService);
        return sslContextService;
    }

    /**
     * Sends numMessages from PutSyslog to ListenSyslog.
     */
    private void run(String protocol, int numMessages, int expectedMessages) throws IOException, InterruptedException {
        // set the same protocol on both processors
        putSyslogRunner.setProperty(PutSyslog.PROTOCOL, protocol);
        listenSyslogRunner.setProperty(ListenSyslog.PROTOCOL, protocol);

        // set a listening port of 0 to get a random available port
        listenSyslogRunner.setProperty(ListenSyslog.PORT, "0");

        // call onScheduled to start ListenSyslog listening
        final ProcessSessionFactory processSessionFactory = listenSyslogRunner.getProcessSessionFactory();
        final ProcessContext context = listenSyslogRunner.getProcessContext();
        listenSyslog.onScheduled(context);

        // get the real port it is listening on and set that in PutSyslog
        final int listeningPort = listenSyslog.getPort();
        putSyslogRunner.setProperty(PutSyslog.PORT, String.valueOf(listeningPort));

        // configure the message properties on PutSyslog
        final String pri = "34";
        final String version = "1";
        final String stamp = "2016-02-05T22:14:15.003Z";
        final String host = "localhost";
        final String body = "some message";
        final String expectedMessage = "<" + pri + ">" + version + " " + stamp + " " + host + " " + body;

        putSyslogRunner.setProperty(PutSyslog.MSG_PRIORITY, pri);
        putSyslogRunner.setProperty(PutSyslog.MSG_VERSION, version);
        putSyslogRunner.setProperty(PutSyslog.MSG_TIMESTAMP, stamp);
        putSyslogRunner.setProperty(PutSyslog.MSG_HOSTNAME, host);
        putSyslogRunner.setProperty(PutSyslog.MSG_BODY, body);

        // send the messages
        for (int i=0; i < numMessages; i++) {
            putSyslogRunner.enqueue("incoming data".getBytes(Charset.forName("UTF-8")));
        }
        putSyslogRunner.run(numMessages, false);

        // trigger ListenSyslog until we've seen all the messages
        int numTransfered = 0;
        long timeout = System.currentTimeMillis() + 30000;

        while (numTransfered < expectedMessages && System.currentTimeMillis() < timeout) {
            Thread.sleep(10);
            listenSyslog.onTrigger(context, processSessionFactory);
            numTransfered = listenSyslogRunner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).size();
        }
        Assert.assertEquals("Did not process all the messages", expectedMessages, numTransfered);

        if (expectedMessages > 0) {
            // check that one of flow files has the expected content
            MockFlowFile mockFlowFile = listenSyslogRunner.getFlowFilesForRelationship(ListenSyslog.REL_SUCCESS).get(0);
            mockFlowFile.assertContentEquals(expectedMessage);
        }
    }

}
