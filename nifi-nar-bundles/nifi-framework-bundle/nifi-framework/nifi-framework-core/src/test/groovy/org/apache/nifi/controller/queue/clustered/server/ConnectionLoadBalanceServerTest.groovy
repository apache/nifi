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
package org.apache.nifi.controller.queue.clustered.server

import org.apache.nifi.events.EventReporter
import org.apache.nifi.reporting.Severity
import org.apache.nifi.security.util.KeyStoreUtils
import org.apache.nifi.security.util.KeystoreType
import org.apache.nifi.security.util.SslContextFactory
import org.apache.nifi.security.util.StandardTlsConfiguration
import org.apache.nifi.security.util.TlsConfiguration
import org.apache.nifi.security.util.TlsPlatform
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLPeerUnverifiedException
import javax.net.ssl.SSLServerSocket
import java.security.Security

@RunWith(JUnit4.class)
class ConnectionLoadBalanceServerTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionLoadBalanceServerTest.class)

    private static final String KEYSTORE_PATH = "src/test/resources/localhost-ks.jks"
    private static final String KEYSTORE_PASSWORD = "OI7kMpWzzVNVx/JGhTL/0uO4+PWpGJ46uZ/pfepbkwI"
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS

    private static final String TRUSTSTORE_PATH = "src/test/resources/localhost-ts.jks"
    private static final String TRUSTSTORE_PASSWORD = "wAOR0nQJ2EXvOP0JZ2EaqA/n7W69ILS4sWAHghmIWCc"
    private static final KeystoreType TRUSTSTORE_TYPE = KeystoreType.JKS

    private static final String HOSTNAME = "localhost"
    private static final int PORT = 54321
    private static final int NUM_THREADS = 1
    private static final int TIMEOUT_MS = 1000

    private static TlsConfiguration tlsConfiguration
    private static SSLContext sslContext

    private ConnectionLoadBalanceServer lbServer

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        tlsConfiguration = new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD, KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE)
        sslContext = SslContextFactory.createSslContext(tlsConfiguration)
    }

    @Before
    void setUp() {
    }

    @After
    void tearDown() {
        if (lbServer) {
            lbServer.stop()
        }
    }

    @Test
    void testRequestPeerListShouldUseTLS() {
        // Arrange
        logger.info("Creating SSL Context from TLS Configuration: ${tlsConfiguration}")
        SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration)
        logger.info("Created SSL Context: ${KeyStoreUtils.sslContextToString(sslContext)}")

        def mockLBP = [
                receiveFlowFiles: { Socket s, InputStream i, OutputStream o -> null }
        ] as LoadBalanceProtocol
        def mockER = [:] as EventReporter

        lbServer = new ConnectionLoadBalanceServer(HOSTNAME, PORT, sslContext, NUM_THREADS, mockLBP, mockER, TIMEOUT_MS)

        // Act
        lbServer.start()

        // Assert

        // Assert that the actual socket is set correctly due to the override in the LB server
        SSLServerSocket socket = lbServer.serverSocket as SSLServerSocket
        assert socket.needClientAuth

        // Clean up
        lbServer.stop()
    }

    @Test
    void testShouldHandleSSLPeerUnverifiedException() {
        // Arrange
        final long testStartMillis = System.currentTimeMillis()
        final int CONNECTION_ATTEMPTS = 100
        // If this test takes longer than 3 seconds, it's likely because of external delays, which would invalidate the assertions
        final long MAX_TEST_DURATION_MILLIS = 3000
        final String peerDescription = "Test peer"
        final SSLPeerUnverifiedException e = new SSLPeerUnverifiedException("Test exception")

        InputStream socketInputStream = new ByteArrayInputStream("This is the socket input stream".bytes)
        OutputStream socketOutputStream = new ByteArrayOutputStream()

        Socket mockSocket = [
                getInputStream : { -> socketInputStream },
                getOutputStream: { -> socketOutputStream },
        ] as Socket
        LoadBalanceProtocol mockLBProtocol = [
                receiveFlowFiles: { Socket s, InputStream i, OutputStream o -> null }
        ] as LoadBalanceProtocol
        EventReporter mockER = [
                reportEvent: { Severity s, String c, String m -> logger.mock("${s}: ${c} | ${m}") }
        ] as EventReporter

        def output = [debug: 0, error: 0]

        ConnectionLoadBalanceServer.CommunicateAction communicateAction = new ConnectionLoadBalanceServer.CommunicateAction(mockLBProtocol, mockSocket, mockER)

        // Override the threshold to 100 ms
        communicateAction.EXCEPTION_THRESHOLD_MILLIS = 100

        long listenerStart = System.currentTimeMillis()

        // Act
        CONNECTION_ATTEMPTS.times { int i ->
            long now = System.currentTimeMillis()
            logger.debug("Attempting connection ${i + 1} at ${now} [${now - listenerStart}]")
            boolean printedError = communicateAction.handleTlsError(peerDescription, e)
            if (printedError) {
                output.error++
            } else {
                output.debug++
            }
            sleep(10)
        }
        logger.info("After ${CONNECTION_ATTEMPTS} attempts, debug: ${output.debug}, error: ${output.error}")

        // Assert
        logger.info("output.debug (${output.debug}) > output.error (${output.error}): ${output.debug > output.error}")

        // Only enforce if the test completed in a reasonable amount of time (i.e. external delays did not influence the timing)
        long testStopMillis = System.currentTimeMillis()
        long testDurationMillis = testStopMillis - testStartMillis
        if (testDurationMillis > MAX_TEST_DURATION_MILLIS) {
            logger.warn("The test took ${testDurationMillis} ms, which is longer than the max duration ${MAX_TEST_DURATION_MILLIS} ms, so the timing may be suspect and the assertion will not be enforced")
        } else {
            assert output.debug > output.error
        }

        // Clean up
        communicateAction.stop()
    }
}
