/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.nifi.processors.email;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.StandardRestrictedSSLContextService;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestListenSMTP {

    private ScheduledExecutorService executor;

    @Before
    public void before() {
        this.executor = Executors.newScheduledThreadPool(2);
    }

    @After
    public void after() {
        this.executor.shutdown();
    }

    @Test
    public void validateSuccessfulInteraction() throws Exception, EmailException {
        int port = NetworkUtils.availablePort();

        TestRunner runner = TestRunners.newTestRunner(ListenSMTP.class);
        runner.setProperty(ListenSMTP.SMTP_PORT, String.valueOf(port));
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");

        runner.assertValid();
        runner.run(5, false);
        final int numMessages = 5;
        CountDownLatch latch = new CountDownLatch(numMessages);

        this.executor.schedule(() -> {
            for (int i = 0; i < numMessages; i++) {
                try {
                    Email email = new SimpleEmail();
                    email.setHostName("localhost");
                    email.setSmtpPort(port);
                    email.setFrom("alice@nifi.apache.org");
                    email.setSubject("This is a test");
                    email.setMsg("MSG-" + i);
                    email.addTo("bob@nifi.apache.org");
                    email.send();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            }
        }, 1500, TimeUnit.MILLISECONDS);

        boolean complete = latch.await(5000, TimeUnit.MILLISECONDS);
        runner.shutdown();
        assertTrue(complete);
        runner.assertAllFlowFilesTransferred(ListenSMTP.REL_SUCCESS, numMessages);
    }

    @Test
    public void validateSuccessfulInteractionWithTls() throws Exception, EmailException {
        System.setProperty("mail.smtp.ssl.trust", "*");
        System.setProperty("javax.net.ssl.keyStore", "src/test/resources/localhost-ks.jks");
        System.setProperty("javax.net.ssl.keyStorePassword", "localtest");
        int port = NetworkUtils.availablePort();

        TestRunner runner = TestRunners.newTestRunner(ListenSMTP.class);
        runner.setProperty(ListenSMTP.SMTP_PORT, String.valueOf(port));
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");

        // Setup the SSL Context
        SSLContextService sslContextService = new StandardRestrictedSSLContextService();
        runner.addControllerService("ssl-context", sslContextService);
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, "src/test/resources/localhost-ts.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, "JKS");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, "src/test/resources/localhost-ks.jks");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, "localtest");
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, "JKS");
        runner.enableControllerService(sslContextService);

        // and add the SSL context to the runner
        runner.setProperty(ListenSMTP.SSL_CONTEXT_SERVICE, "ssl-context");
        runner.setProperty(ListenSMTP.CLIENT_AUTH, SSLContextService.ClientAuth.NONE.name());
        runner.assertValid();

        int messageCount = 5;
        CountDownLatch latch = new CountDownLatch(messageCount);
        runner.run(messageCount, false);

        this.executor.schedule(() -> {
            for (int i = 0; i < messageCount; i++) {
                try {
                    Email email = new SimpleEmail();
                    email.setHostName("localhost");
                    email.setSmtpPort(port);
                    email.setFrom("alice@nifi.apache.org");
                    email.setSubject("This is a test");
                    email.setMsg("MSG-" + i);
                    email.addTo("bob@nifi.apache.org");

                    // Enable STARTTLS but ignore the cert
                    email.setStartTLSEnabled(true);
                    email.setStartTLSRequired(true);
                    email.setSSLCheckServerIdentity(false);
                    email.send();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            }
        }, 1500, TimeUnit.MILLISECONDS);

        boolean complete = latch.await(5000, TimeUnit.MILLISECONDS);
        runner.shutdown();
        assertTrue(complete);
        runner.assertAllFlowFilesTransferred("success", messageCount);
    }

    @Test
    public void validateTooLargeMessage() throws Exception, EmailException {
        int port = NetworkUtils.availablePort();

        TestRunner runner = TestRunners.newTestRunner(ListenSMTP.class);
        runner.setProperty(ListenSMTP.SMTP_PORT, String.valueOf(port));
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_MSG_SIZE, "10 B");

        runner.assertValid();

        int messageCount = 1;
        CountDownLatch latch = new CountDownLatch(messageCount);

        runner.run(messageCount, false);

        this.executor.schedule(() -> {
            for (int i = 0; i < messageCount; i++) {
                try {
                    Email email = new SimpleEmail();
                    email.setHostName("localhost");
                    email.setSmtpPort(port);
                    email.setFrom("alice@nifi.apache.org");
                    email.setSubject("This is a test");
                    email.setMsg("MSG-" + i);
                    email.addTo("bob@nifi.apache.org");
                    email.send();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            }
        }, 1000, TimeUnit.MILLISECONDS);

        boolean complete = latch.await(5000, TimeUnit.MILLISECONDS);
        runner.shutdown();
        assertTrue(complete);
        runner.assertAllFlowFilesTransferred("success", 0);
    }
}
