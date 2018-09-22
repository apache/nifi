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
package org.apache.nifi.processors.standard

import groovy.servlet.GroovyServlet
import groovy.test.GroovyAssert
import org.apache.nifi.ssl.SSLContextService
import org.apache.nifi.ssl.StandardSSLContextService
import org.apache.nifi.util.StandardProcessorTestRunner
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.HttpConnectionFactory
import org.eclipse.jetty.server.SecureRequestCustomizer
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.SslConnectionFactory
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import sun.security.ssl.SSLContextImpl
import sun.security.ssl.SSLEngineImpl

import javax.crypto.Cipher
import javax.net.SocketFactory
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLHandshakeException
import javax.net.ssl.SSLSocket
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import java.security.Security

@RunWith(JUnit4.class)
class TestGetHTTPGroovy extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TestGetHTTPGroovy.class)

    static private final String KEYSTORE_TYPE = "JKS"

    private static final String TLSv1 = "TLSv1"
    private static final String TLSv1_1 = "TLSv1.1"
    private static final String TLSv1_2 = "TLSv1.2"
    private static final List DEFAULT_PROTOCOLS = [TLSv1, TLSv1_1, TLSv1_2]

    private static final String TLSv1_1_CIPHER_SUITE = "TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA"
    private static
    final List DEFAULT_CIPHER_SUITES = new SSLEngineImpl(new SSLContextImpl.TLSContext()).supportedCipherSuites as List

    private static final String DEFAULT_HOSTNAME = "localhost"
    private static final int DEFAULT_TLS_PORT = 8456
    private static final String HTTPS_URL = "https://${DEFAULT_HOSTNAME}:${DEFAULT_TLS_PORT}"
    private static final String GET_URL = "${HTTPS_URL}/GetHandler.groovy"

    private static final String MOZILLA_INTERMEDIATE_URL = "https://mozilla-intermediate.badssl.com/"
    private static final String TLS_1_URL = "https://nifi.apache.org/"
    private static final String TLS_1_1_URL = "https://nifi.apache.org/"

    private static final String KEYSTORE_PATH = "src/test/resources/keystore.jks"
    private static final String TRUSTSTORE_PATH = "src/test/resources/truststore.jks"
    private static final String CACERTS_PATH = "/Library/Java/JavaVirtualMachines/jdk1.8.0_101.jdk/Contents/Home/jre/lib/security/cacerts"

    private static final String KEYSTORE_PASSWORD = "passwordpassword"
    private static final String TRUSTSTORE_PASSWORD = "passwordpassword"
    private static final String CACERTS_PASSWORD = "changeit"

    private static Server server
    private static X509TrustManager nullTrustManager
    private static HostnameVerifier nullHostnameVerifier

    private static TestRunner runner

    private
    static Server createServer(List supportedProtocols = DEFAULT_PROTOCOLS, List supportedCipherSuites = DEFAULT_CIPHER_SUITES) {
        // Create Server
        Server server = new Server()

        // Add some secure config
        final HttpConfiguration httpsConfiguration = new HttpConfiguration()
        httpsConfiguration.setSecureScheme("https")
        httpsConfiguration.setSecurePort(DEFAULT_TLS_PORT)
        httpsConfiguration.addCustomizer(new SecureRequestCustomizer())

        // Build the TLS connector
        final ServerConnector https = createConnector(server, httpsConfiguration, supportedProtocols, supportedCipherSuites)

        // Add this connector
        server.addConnector(https)
        logger.info("Created server with supported protocols: ${supportedProtocols}")

        /** Create a simple Groovlet that responds to the incoming request by reversing the string parameter
         * i.e. localhost:8456/ReverseHandler.groovy?string=Happy%20birthday -> yadhtrib yppaH
         */
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
        context.with {
            contextPath = '/'
            resourceBase = 'src/test/resources/TestGetHTTP'
            addServlet(GroovyServlet, '*.groovy')
        }
        server.setHandler(context)
        server
    }

    private
    static ServerConnector createConnector(Server server, HttpConfiguration httpsConfiguration, List supportedProtocols = DEFAULT_PROTOCOLS, List supportedCipherSuites = DEFAULT_CIPHER_SUITES) {
        ServerConnector https = new ServerConnector(server,
                new SslConnectionFactory(createSslContextFactory(supportedProtocols, supportedCipherSuites), "http/1.1"),
                new HttpConnectionFactory(httpsConfiguration))

        // set host and port
        https.setHost(DEFAULT_HOSTNAME)
        https.setPort(DEFAULT_TLS_PORT)
        https
    }

    private
    static SslContextFactory createSslContextFactory(List supportedProtocols = DEFAULT_PROTOCOLS, List supportedCipherSuites = DEFAULT_CIPHER_SUITES) {
        final SslContextFactory contextFactory = new SslContextFactory()
        contextFactory.needClientAuth = false
        contextFactory.wantClientAuth = false

        contextFactory.setKeyStorePath(KEYSTORE_PATH)
        contextFactory.setKeyStoreType(KEYSTORE_TYPE)
        contextFactory.setKeyStorePassword(KEYSTORE_PASSWORD)

        contextFactory.setIncludeProtocols(supportedProtocols as String[])
        if (supportedCipherSuites) {
            contextFactory.setIncludeCipherSuites(supportedCipherSuites as String[])
        }
        contextFactory
    }

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        server = createServer()

        runner = configureRunner()

        // Print the default cipher suite list
        logger.info("Default supported cipher suites: \n\t${DEFAULT_CIPHER_SUITES.join("\n\t")}")
    }

    private static TestRunner configureRunner() {
        // Set the default trust manager for the "default" tests (the outgoing Groovy call) to ignore certificate path verification for localhost
        nullTrustManager = [
                checkClientTrusted: { chain, authType -> },
                checkServerTrusted: { chain, authType -> },
                getAcceptedIssuers: { null }
        ] as X509TrustManager

        nullHostnameVerifier = [
                verify: { String hostname, session ->
                    // Will always return true if the hostname is "localhost" or the Mozilla intermediate site
                    hostname.equalsIgnoreCase(DEFAULT_HOSTNAME) || hostname.equalsIgnoreCase(new URL(MOZILLA_INTERMEDIATE_URL).host)
                }
        ] as HostnameVerifier

        // Configure the test runner
        TestRunner runner = TestRunners.newTestRunner(GetHTTP.class)
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService("ssl-context", sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, TRUSTSTORE_PATH)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, KEYSTORE_TYPE)
        runner.enableControllerService(sslContextService)

        runner.setProperty(GetHTTP.URL, GET_URL)
        runner.setProperty(GetHTTP.SSL_CONTEXT_SERVICE, "ssl-context")

        runner
    }

    @AfterClass
    static void tearDownOnce() {

    }

    @Before
    void setUp() throws Exception {
        // This must be executed before each test, or the connections will be re-used and if a TLSv1.1 connection is re-used against a server that only supports TLSv1.2, it will fail
        SSLContext sc = SSLContext.getInstance(TLSv1_2)
        sc.init(null, [nullTrustManager] as TrustManager[], null)
        SocketFactory socketFactory = sc.getSocketFactory()
        logger.info("JCE unlimited strength installed: ${Cipher.getMaxAllowedKeyLength("AES") > 128}")
        logger.info("Supported client cipher suites: ${socketFactory.supportedCipherSuites}")
        HttpsURLConnection.setDefaultSSLSocketFactory(socketFactory)
        HttpsURLConnection.setDefaultHostnameVerifier(nullHostnameVerifier)

        runner.setProperty(GetHTTP.FILENAME, "mockFlowfile_${System.currentTimeMillis()}")

        (runner as StandardProcessorTestRunner).clearQueue()
    }

    @After
    void tearDown() throws Exception {
        try {
            server.stop()
        } catch (Exception e) {
            e.printStackTrace()
        }

        runner.clearTransferState()
        runner.clearProvenanceEvents()
        (runner as StandardProcessorTestRunner).clearQueue()
    }

    /**
     * Jetty 9.4.0+ no longer supports TLSv1.
     */
    @Test
    @Ignore("Ignore until embeddable test HTTPS server that supports TLSv1 is available")
    void testDefaultShouldSupportTLSv1() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/ReverseHandler.groovy?string=${URLEncoder.encode(MSG, "UTF-8")}"

        // Configure server with TLSv1 only
        server = createServer([TLSv1])

        // Start server
        server.start()

        // Act
        String response = new URL(url).text
        logger.info("Response from ${HTTPS_URL}: ${response}")

        // Assert
        assert response == MSG.reverse()
    }

    /**
     * Jetty 9.4.0+ no longer supports TLSv1.1 unless compatible ("vulnerable" according to Jetty documentation, see <a href="https://github.com/eclipse/jetty.project/issues/860">https://github.com/eclipse/jetty.project/issues/860</a>) cipher suites are explicitly provided.
     */
    @Test
    @Ignore("Ignore until embeddable test HTTPS server that supports TLSv1.1 is available")
    void testDefaultShouldSupportTLSv1_1() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/ReverseHandler.groovy?string=${URLEncoder.encode(MSG, "UTF-8")}"

        // Configure server with TLSv1.1 only
        server = createServer([TLSv1_1])

        // Start server
        server.start()

        // Act
        String response = new URL(url).text
        logger.info("Response from ${HTTPS_URL}: ${response}")

        // Assert
        assert response == MSG.reverse()
    }

    /**
     * Jetty 9.4.0+ no longer supports TLSv1.1 unless compatible ("vulnerable" according to Jetty documentation, see <a href="https://github.com/eclipse/jetty.project/issues/860">https://github.com/eclipse/jetty.project/issues/860</a>) cipher suites are explicitly provided.
     */
    @Test
    @Ignore("Ignore until embeddable test HTTPS server that supports TLSv1 is available")
    void testDefaultShouldSupportTLSv1_1WithVulnerableCipherSuites() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/ReverseHandler.groovy?string=${URLEncoder.encode(MSG, "UTF-8")}"

        // Configure server with TLSv1.1 only but explicitly provide the legacy cipher suite
        server = createServer([TLSv1_1], [TLSv1_1_CIPHER_SUITE])

        // Start server
        server.start()

        // Act
        String response = new URL(url).text
        logger.info("Response from ${HTTPS_URL}: ${response}")

        // Assert
        assert response == MSG.reverse()
    }

    @Test
    void testDefaultShouldSupportTLSv1_2() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/ReverseHandler.groovy?string=${URLEncoder.encode(MSG, "UTF-8")}"

        // Configure server with TLSv1.2 only
        server = createServer([TLSv1_2])

        // Start server
        server.start()

        // Act
        String response = new URL(url).text
        logger.info("Response from ${HTTPS_URL}: ${response}")

        // Assert
        assert response == MSG.reverse()
    }

    @Test
    void testDefaultShouldPreferTLSv1_2() {
        // Arrange
        final String MSG = "This is a test message"
        final String url = "${HTTPS_URL}/ReverseHandler.groovy?string=${URLEncoder.encode(MSG, "UTF-8")}"

        // Configure server with all TLS protocols
        server = createServer()

        // Start server
        server.start()

        // Create a connection that could use TLSv1, TLSv1.1, or TLSv1.2
        SSLContext sc = SSLContext.getInstance("TLS")
        sc.init(null, [nullTrustManager] as TrustManager[], null)
        SocketFactory socketFactory = sc.getSocketFactory()

        URL formedUrl = new URL(url)
        SSLSocket socket = (SSLSocket) socketFactory.createSocket(formedUrl.host, formedUrl.port)
        logger.info("Enabled protocols: ${socket.enabledProtocols}")

        // Act
        socket.startHandshake()
        String selectedProtocol = socket.getSession().protocol
        logger.info("Selected protocol: ${selectedProtocol}")

        // Assert
        assert selectedProtocol == TLSv1_2
    }

    private static void enableContextServiceProtocol(TestRunner runner, String protocol, String truststorePath = TRUSTSTORE_PATH, String truststorePassword = TRUSTSTORE_PASSWORD) {
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService("ssl-context", sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, truststorePath)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, truststorePassword)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, KEYSTORE_TYPE)
        runner.setProperty(sslContextService, StandardSSLContextService.SSL_ALGORITHM, protocol)
        runner.enableControllerService(sslContextService)
        logger.info("GetHTTP supported protocols: ${sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE).protocol}")
        logger.info("GetHTTP supported cipher suites: ${sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE).supportedSSLParameters.cipherSuites}")
    }

    /**
     * This test connects to a server running TLSv1/1.1/1.2. It iterates over an {@link SSLContextService} with TLSv1, TLSv1.1, and TLSv1.2 support. All three context services should be able to communicate successfully.
     */
    @Test
    @Ignore("Ignore until embeddable test HTTPS server that supports TLSv1 is available")
    void testGetHTTPShouldConnectToServerWithTLSv1() {
        // Arrange

        // Connect to a server that still runs TLSv1/1.1/1.2
        runner.setProperty(GetHTTP.URL, TLS_1_URL)

        // Act
        [TLSv1, TLSv1_1, TLSv1_2].each { String tlsVersion ->
            logger.info("Set context service protocol to ${tlsVersion}")
            enableContextServiceProtocol(runner, tlsVersion, CACERTS_PATH, CACERTS_PASSWORD)
            runner.assertQueueEmpty()
            logger.info("Queue size (before run): ${runner.queueSize}")
            runner.run()

            // Assert
            logger.info("Queue size (after run): ${runner.queueSize}")
            runner.assertAllFlowFilesTransferred(GetHTTP.REL_SUCCESS, 1)
            runner.clearTransferState()
            logger.info("Ran successfully")
        }
    }

    /**
     * This test creates a server that supports TLSv1.1. It iterates over an {@link SSLContextService} with TLSv1, TLSv1.1, and TLSv1.2 support. The context service with TLSv1 should not be able to communicate with a server that does not support it, but TLSv1.1 and TLSv1.2 should be able to communicate successfully.
     */
    @Test
    @Ignore("Ignore until embeddable test HTTPS server that only supports TLSv1.1 is available")
    void testGetHTTPShouldConnectToServerWithTLSv1_1() {
        // Arrange
        final String MSG = "This is a test message"

        // Configure server with TLSv1.1 only
        server = createServer([TLSv1_1])

        // Start server
        server.start()

        // Act
        logger.info("Set context service protocol to ${TLSv1}")
        enableContextServiceProtocol(runner, TLSv1)
        runner.enqueue(MSG.getBytes())

        def ae = GroovyAssert.shouldFail(AssertionError) {
            runner.run()
        }
        logger.expected(ae.getMessage())
        logger.expected("Cause: ${ae.cause?.class?.name}")
        logger.expected("Original cause: ${ae.cause?.cause?.class?.name}")

        // Assert
        assert ae.cause.cause instanceof SSLHandshakeException
        runner.clearTransferState()
        logger.expected("Unable to connect")

        [TLSv1_1, TLSv1_2].each { String tlsVersion ->
            logger.info("Set context service protocol to ${tlsVersion}")
            enableContextServiceProtocol(runner, tlsVersion)
            runner.enqueue(MSG.getBytes())
            runner.run()

            // Assert
            runner.assertAllFlowFilesTransferred(GetHTTP.REL_SUCCESS)
            runner.clearTransferState()
            logger.info("Ran successfully")
        }
    }

    /**
     * This test creates a server that supports TLSv1.2. It iterates over an {@link SSLContextService} with TLSv1, TLSv1.1, and TLSv1.2 support. The context services with TLSv1 and TLSv1.1 should not be able to communicate with a server that does not support it, but TLSv1.2 should be able to communicate successfully.
     */
    @Test
    void testGetHTTPShouldConnectToServerWithTLSv1_2() {
        // Arrange
        final String MSG = "This is a test message"

        // Configure server with TLSv1.2 only
        server = createServer([TLSv1_2])

        // Start server
        server.start()

        // Act
        [TLSv1, TLSv1_1].each { String tlsVersion ->
            logger.info("Set context service protocol to ${tlsVersion}")
            enableContextServiceProtocol(runner, tlsVersion)
            runner.enqueue(MSG.getBytes())
            def ae = GroovyAssert.shouldFail(AssertionError) {
                runner.run()
            }
            logger.expected(ae.getMessage())
            logger.expected("Cause: ${ae.cause?.class?.name}")
            logger.expected("Original cause: ${ae.cause?.cause?.class?.name}")

            // Assert
            assert ae.cause.cause instanceof SSLHandshakeException
            runner.clearTransferState()
            logger.expected("Unable to connect")
        }

        logger.info("Set context service protocol to ${TLSv1_2}")
        enableContextServiceProtocol(runner, TLSv1_2)
        runner.enqueue(MSG.getBytes())
        runner.run()

        // Assert
        runner.assertAllFlowFilesTransferred(GetHTTP.REL_SUCCESS)
        runner.clearTransferState()
        logger.info("Ran successfully")
    }
}