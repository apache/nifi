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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;

public class TestListenTCP {

    private ListenTCP proc;
    private TestRunner runner;

    @Before
    public void setup() {
        proc = new ListenTCP();
        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(ListenTCP.PORT, "0");
    }

    @Test
    public void testCustomValidate() throws InitializationException {
        runner.setProperty(ListenTCP.PORT, "1");
        runner.assertValid();

        configureProcessorSslContextService();
        runner.setProperty(ListenTCP.CLIENT_AUTH, "");
        runner.assertNotValid();

        runner.setProperty(ListenTCP.CLIENT_AUTH, SslContextFactory.ClientAuth.REQUIRED.name());
        runner.assertValid();
    }

    @Test
    public void testListenTCP() throws IOException, InterruptedException {
        final List<String> messages = new ArrayList<>();
        messages.add("This is message 1\n");
        messages.add("This is message 2\n");
        messages.add("This is message 3\n");
        messages.add("This is message 4\n");
        messages.add("This is message 5\n");

        runTCP(messages, messages.size(), null);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        for (int i=0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals("This is message " + (i + 1));
        }
    }

    @Test
    public void testListenTCPBatching() throws IOException, InterruptedException {
        runner.setProperty(ListenTCP.MAX_BATCH_SIZE, "3");

        final List<String> messages = new ArrayList<>();
        messages.add("This is message 1\n");
        messages.add("This is message 2\n");
        messages.add("This is message 3\n");
        messages.add("This is message 4\n");
        messages.add("This is message 5\n");

        runTCP(messages, 2, null);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);

        MockFlowFile mockFlowFile1 = mockFlowFiles.get(0);
        mockFlowFile1.assertContentEquals("This is message 1\nThis is message 2\nThis is message 3");

        MockFlowFile mockFlowFile2 = mockFlowFiles.get(1);
        mockFlowFile2.assertContentEquals("This is message 4\nThis is message 5");
    }

    @Test
    public void testTLSClientAuthRequiredAndClientCertProvided() throws InitializationException, IOException, InterruptedException,
            UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        runner.setProperty(ListenTCP.CLIENT_AUTH, SSLContextService.ClientAuth.REQUIRED.name());
        configureProcessorSslContextService();

        final List<String> messages = new ArrayList<>();
        messages.add("This is message 1\n");
        messages.add("This is message 2\n");
        messages.add("This is message 3\n");
        messages.add("This is message 4\n");
        messages.add("This is message 5\n");

        // Make an SSLContext with a key and trust store to send the test messages
        final SSLContext clientSslContext = SslContextFactory.createSslContext(
                "src/test/resources/localhost-ks.jks",
                "localtest".toCharArray(),
                "jks",
                "src/test/resources/localhost-ts.jks",
                "localtest".toCharArray(),
                "jks",
                org.apache.nifi.security.util.SslContextFactory.ClientAuth.valueOf("NONE"),
                "TLS");

        runTCP(messages, messages.size(), clientSslContext);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        for (int i=0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals("This is message " + (i + 1));
        }
    }

    @Test
    public void testTLSClientAuthRequiredAndClientCertNotProvided() throws InitializationException, IOException, InterruptedException,
            UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        runner.setProperty(ListenTCP.CLIENT_AUTH, SSLContextService.ClientAuth.REQUIRED.name());
        configureProcessorSslContextService();

        final List<String> messages = new ArrayList<>();
        messages.add("This is message 1\n");
        messages.add("This is message 2\n");
        messages.add("This is message 3\n");
        messages.add("This is message 4\n");
        messages.add("This is message 5\n");

        // Make an SSLContext that only has the trust store, this should not work since the processor has client auth REQUIRED
        final SSLContext clientSslContext = SslContextFactory.createTrustSslContext(
                "src/test/resources/localhost-ts.jks",
                "localtest".toCharArray(),
                "jks",
                "TLS");

        try {
            runTCP(messages, messages.size(), clientSslContext);
            Assert.fail("Should have thrown exception");
        } catch (Exception e) {

        }
    }

    @Test
    public void testTLSClientAuthNoneAndClientCertNotProvided() throws InitializationException, IOException, InterruptedException,
            UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

        runner.setProperty(ListenTCP.CLIENT_AUTH, SSLContextService.ClientAuth.NONE.name());
        configureProcessorSslContextService();

        final List<String> messages = new ArrayList<>();
        messages.add("This is message 1\n");
        messages.add("This is message 2\n");
        messages.add("This is message 3\n");
        messages.add("This is message 4\n");
        messages.add("This is message 5\n");

        // Make an SSLContext that only has the trust store, this should not work since the processor has client auth REQUIRED
        final SSLContext clientSslContext = SslContextFactory.createTrustSslContext(
                "src/test/resources/localhost-ts.jks",
                "localtest".toCharArray(),
                "jks",
                "TLS");

        runTCP(messages, messages.size(), clientSslContext);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS);
        for (int i=0; i < mockFlowFiles.size(); i++) {
            mockFlowFiles.get(i).assertContentEquals("This is message " + (i + 1));
        }
    }

    protected void runTCP(final List<String> messages, final int expectedTransferred, final SSLContext sslContext)
            throws IOException, InterruptedException {

        Socket socket = null;
        try {
            // schedule to start listening on a random port
            final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
            final ProcessContext context = runner.getProcessContext();
            proc.onScheduled(context);

            // create a client connection to the port the dispatcher is listening on
            final int realPort = proc.getDispatcherPort();

            // create either a regular socket or ssl socket based on context being passed in
            if (sslContext != null) {
                socket = sslContext.getSocketFactory().createSocket("localhost", realPort);
            } else {
                socket = new Socket("localhost", realPort);
            }
            Thread.sleep(100);

            // send the frames to the port the processors is listening on
            for (final String message : messages) {
                socket.getOutputStream().write(message.getBytes(StandardCharsets.UTF_8));
                Thread.sleep(1);
            }
            socket.getOutputStream().flush();

            long responseTimeout = 10000;

            // this first loop waits until the internal queue of the processor has the expected
            // number of messages ready before proceeding, we want to guarantee they are all there
            // before onTrigger gets a chance to run
            long startTimeQueueSizeCheck = System.currentTimeMillis();
            while (proc.getQueueSize() < messages.size()
                    && (System.currentTimeMillis() - startTimeQueueSizeCheck < responseTimeout)) {
                Thread.sleep(100);
            }

            // want to fail here if the queue size isn't what we expect
            Assert.assertEquals(messages.size(), proc.getQueueSize());

            // call onTrigger until we processed all the frames, or a certain amount of time passes
            int numTransferred = 0;
            long startTime = System.currentTimeMillis();
            while (numTransferred < expectedTransferred  && (System.currentTimeMillis() - startTime < responseTimeout)) {
                proc.onTrigger(context, processSessionFactory);
                numTransferred = runner.getFlowFilesForRelationship(ListenTCP.REL_SUCCESS).size();
                Thread.sleep(100);
            }

            // should have transferred the expected events
            runner.assertTransferCount(ListenTCP.REL_SUCCESS, expectedTransferred);
        } finally {
            // unschedule to close connections
            proc.onUnscheduled();
            IOUtils.closeQuietly(socket);
        }
    }

    private SSLContextService configureProcessorSslContextService() throws InitializationException {
        final SSLContextService sslContextService = new StandardRestrictedSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/localhost-ts.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/localhost-ks.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");
        runner.enableControllerService(sslContextService);

        runner.setProperty(ListenTCP.SSL_CONTEXT_SERVICE, "ssl-context");
        return sslContextService;
    }

}
