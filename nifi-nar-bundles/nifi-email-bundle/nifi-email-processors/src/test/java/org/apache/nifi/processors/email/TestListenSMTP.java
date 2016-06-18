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

import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.apache.nifi.ssl.SSLContextService;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestListenSMTP {

    @Test(timeout=15000)
    public void ValidEmailTls() throws Exception {
        boolean[] failed = {false};
        ListenSMTP listenSmtp = new ListenSMTP();
        final TestRunner runner = TestRunners.newTestRunner(listenSmtp);

        runner.setProperty(ListenSMTP.SMTP_PORT, "0");
        runner.setProperty(ListenSMTP.SMTP_HOSTNAME, "bermudatriangle");
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");
        runner.setProperty(ListenSMTP.SMTP_TIMEOUT, "10 seconds");

        // Setup the SSL Context
        final SSLContextService sslContextService = new StandardSSLContextService();
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



        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();

        // NOTE: This test routine uses  the same strategy used by TestListenAndPutSyslog
        // where listenSmtp method calls are used to allow the processor to be started using
        // port "0" without triggering a violation of PORT_VALIDATOR

        listenSmtp.onScheduled(context);
        listenSmtp.initializeSMTPServer(context);

        final int port = listenSmtp.getPort();

        try {
            final Thread clientThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {


                        System.setProperty("mail.smtp.ssl.trust", "*");
                        System.setProperty("javax.net.ssl.keyStore", "src/test/resources/localhost-ks.jks");
                        System.setProperty("javax.net.ssl.keyStorePassword", "localtest");

                        Email email = new SimpleEmail();

                        email.setHostName("127.0.0.1");
                        email.setSmtpPort(port);

                        // Enable STARTTLS but ignore the cert
                        email.setStartTLSEnabled(true);
                        email.setStartTLSRequired(true);
                        email.setSSLCheckServerIdentity(false);

                        email.setFrom("alice@nifi.apache.org");
                        email.setSubject("This is a test");
                        email.setMsg("Test test test chocolate");
                        email.addTo("bob@nifi.apache.org");

                        email.send();
                    } catch (final Throwable t) {
                        failed[0] = true;
                    }
                }
            });
            clientThread.start();

            while (runner.getFlowFilesForRelationship(ListenSMTP.REL_SUCCESS).isEmpty()) {
                // process the request.
                listenSmtp.onTrigger(context, processSessionFactory);
            }

                // Checks if client experienced Exception
                Assert.assertFalse("Client experienced exception", failed[0]);

            runner.assertTransferCount(ListenSMTP.REL_SUCCESS, 1);
            clientThread.stop();

            Assert.assertFalse("Sending email failed", failed[0]);

            runner.assertQueueEmpty();
            final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ListenSMTP.REL_SUCCESS);
            splits.get(0).assertAttributeEquals("smtp.from", "alice@nifi.apache.org");
            splits.get(0).assertAttributeEquals("smtp.to", "bob@nifi.apache.org");

            Thread.sleep(100);
        } finally {
            // shut down the server
            listenSmtp.startShutdown();
        }
    }

    @Test(timeout=15000)
    public void ValidEmail() throws Exception, EmailException {
        final boolean[] failed = {false};
        ListenSMTP listenSmtp = new ListenSMTP();
        final TestRunner runner = TestRunners.newTestRunner(listenSmtp);

        runner.setProperty(ListenSMTP.SMTP_PORT, "0");
        runner.setProperty(ListenSMTP.SMTP_HOSTNAME, "bermudatriangle");
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");
        runner.setProperty(ListenSMTP.SMTP_TIMEOUT, "10 seconds");

        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();

        // NOTE: This test routine uses  the same strategy used by TestListenAndPutSyslog
        // where listenSmtp method calls are used to allow the processor to be started using
        // port "0" without triggering a violation of PORT_VALIDATOR
        listenSmtp.onScheduled(context);
        listenSmtp.initializeSMTPServer(context);

        final int port = listenSmtp.getPort();

        try {
            final Thread clientThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Email email = new SimpleEmail();
                        email.setHostName("127.0.0.1");
                        email.setSmtpPort(port);
                        email.setStartTLSEnabled(false);
                        email.setFrom("alice@nifi.apache.org");
                        email.setSubject("This is a test");
                        email.setMsg("Test test test chocolate");
                        email.addTo("bob@nifi.apache.org");
                        email.send();

                    } catch (final EmailException t) {
                        failed[0] = true;
                    }
                }
            });
            clientThread.start();

            while (runner.getFlowFilesForRelationship(ListenSMTP.REL_SUCCESS).isEmpty()) {
                // process the request.
                listenSmtp.onTrigger(context, processSessionFactory);
            }
            clientThread.stop();

            Assert.assertFalse("Sending email failed", failed[0]);

            runner.assertTransferCount(ListenSMTP.REL_SUCCESS, 1);

            runner.assertQueueEmpty();
            final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ListenSMTP.REL_SUCCESS);
            splits.get(0).assertAttributeEquals("smtp.from", "alice@nifi.apache.org");
            splits.get(0).assertAttributeEquals("smtp.to", "bob@nifi.apache.org");

            Thread.sleep(100);
        } finally {
                // shut down the server
                listenSmtp.startShutdown();
        }
    }

    @Test(timeout=15000, expected=EmailException.class)
    public void ValidEmailTimeOut() throws Exception {

        ListenSMTP listenSmtp = new ListenSMTP();
        final TestRunner runner = TestRunners.newTestRunner(listenSmtp);

        runner.setProperty(ListenSMTP.SMTP_PORT, "0");
        runner.setProperty(ListenSMTP.SMTP_HOSTNAME, "bermudatriangle");
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "3");
        runner.setProperty(ListenSMTP.SMTP_TIMEOUT, "50 milliseconds");

        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();

        // NOTE: This test routine uses  the same strategy used by TestListenAndPutSyslog
        // where listenSmtp method calls are used to allow the processor to be started using
        // port "0" without triggering a violation of PORT_VALIDATOR
        listenSmtp.onScheduled(context);
        listenSmtp.initializeSMTPServer(context);

        final int port = listenSmtp.getPort();


        Email email = new SimpleEmail();
        email.setHostName("127.0.0.1");
        email.setSmtpPort(port);
        email.setStartTLSEnabled(false);
        email.setFrom("alice@nifi.apache.org");
        email.setSubject("This is a test");
        email.setMsg("Test test test chocolate");
        email.addTo("bob@nifi.apache.org");
        email.send();

        while (runner.getFlowFilesForRelationship(ListenSMTP.REL_SUCCESS).isEmpty()) {
            // force timeout
            Thread.sleep(999L);
            // process the request.
            listenSmtp.onTrigger(context, processSessionFactory);
        }

        runner.assertQueueEmpty();
        final List<MockFlowFile> splits = runner.getFlowFilesForRelationship(ListenSMTP.REL_SUCCESS);
        splits.get(0).assertAttributeEquals("smtp.from", "alice@nifi.apache.org");
        splits.get(0).assertAttributeEquals("smtp.to", "bob@nifi.apache.org");

        Thread.sleep(100);

        // shut down the server
        listenSmtp.startShutdown();
    }

    @Test(timeout=15000, expected=EmailException.class)
    public void emailTooLarge() throws Exception {
        ListenSMTP listenSmtp = new ListenSMTP();
        final TestRunner runner = TestRunners.newTestRunner(listenSmtp);

        runner.setProperty(ListenSMTP.SMTP_PORT, "0");
        runner.setProperty(ListenSMTP.SMTP_HOSTNAME, "bermudatriangle");
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_MSG_SIZE, "256B");
        runner.setProperty(ListenSMTP.SMTP_MAXIMUM_CONNECTIONS, "2");
        runner.setProperty(ListenSMTP.SMTP_TIMEOUT, "10 seconds");

        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();

        // NOTE: This test routine uses  the same strategy used by TestListenAndPutSyslog
        // where listenSmtp method calls are used to allow the processor to be started using
        // port "0" without triggering a violation of PORT_VALIDATOR
        listenSmtp.onScheduled(context);
        listenSmtp.initializeSMTPServer(context);

        final int port = listenSmtp.getPort();

        Email email = new SimpleEmail();
        email.setHostName("127.0.0.1");
        email.setSmtpPort(port);
        email.setStartTLSEnabled(false);
        email.setFrom("alice@nifi.apache.org");
        email.setSubject("This is a test");
        email.setMsg("Test test test chocolate");
        email.addTo("bob@nifi.apache.org");
        email.send();

        Thread.sleep(100);


        // process the request.
        listenSmtp.onTrigger(context, processSessionFactory);

        runner.assertTransferCount(ListenSMTP.REL_SUCCESS, 0);
        runner.assertQueueEmpty();

        try {
                listenSmtp.startShutdown();
        } catch (InterruptedException e) {
                e.printStackTrace();
                Assert.assertFalse(e.toString(), true);
        }
    }
}