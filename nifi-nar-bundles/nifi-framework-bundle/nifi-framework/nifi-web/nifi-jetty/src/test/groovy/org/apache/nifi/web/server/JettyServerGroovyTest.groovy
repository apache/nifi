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
package org.apache.nifi.web.server

import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.spi.LoggingEvent
import org.apache.nifi.bundle.Bundle
import org.apache.nifi.nar.ExtensionManagerHolder
import org.apache.nifi.nar.ExtensionMapping
import org.apache.nifi.nar.SystemBundle
import org.apache.nifi.processor.DataUnit
import org.apache.nifi.remote.io.socket.NetworkUtils
import org.apache.nifi.security.util.StandardTlsConfiguration
import org.apache.nifi.security.util.TlsConfiguration
import org.apache.nifi.security.util.TlsPlatform
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.eclipse.jetty.server.Connector
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.SslConnectionFactory
import org.eclipse.jetty.servlet.FilterHolder
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.webapp.WebAppContext
import org.junit.After
import org.junit.AfterClass
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.contrib.java.lang.system.Assertion
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.contrib.java.lang.system.SystemErrRule
import org.junit.contrib.java.lang.system.SystemOutRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory
import javax.servlet.DispatcherType
import java.nio.charset.StandardCharsets
import java.security.Security

@RunWith(JUnit4.class)
class JettyServerGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(JettyServerGroovyTest.class)

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog()

    @Rule
    public final SystemErrRule systemErrRule = new SystemErrRule().enableLog()

    private static final int DEFAULT_HTTP_PORT = 8080
    private static final int DEFAULT_HTTPS_PORT = 8443

    private static final int HTTPS_PORT = NetworkUtils.getAvailableTcpPort()
    private static final String HTTPS_HOSTNAME = "localhost"

    private static final String KEYSTORE_PATH = "src/test/resources/keystore.jks"
    private static final String TRUSTSTORE_PATH = "src/test/resources/truststore.jks"
    private static final String STORE_PASSWORD = "passwordpassword"
    private static final String STORE_TYPE = "JKS"

    private static final String TLS_1_3_PROTOCOL = "TLSv1.3"
    private static final List<String> TLS_1_3_CIPHER_SUITES = ["TLS_AES_128_GCM_SHA256"]

    // These protocol versions should not ever be supported
    static private final List<String> LEGACY_TLS_PROTOCOLS = ["TLS", "TLSv1", "TLSv1.1", "SSL", "SSLv2", "SSLv2Hello", "SSLv3"]

    NiFiProperties httpsProps = new NiFiProperties(new Properties([
            (NiFiProperties.WEB_HTTPS_PORT)            : HTTPS_PORT as String,
            (NiFiProperties.WEB_HTTPS_HOST)            : HTTPS_HOSTNAME,
            (NiFiProperties.SECURITY_KEYSTORE)         : KEYSTORE_PATH,
            (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : STORE_PASSWORD,
            (NiFiProperties.SECURITY_KEYSTORE_TYPE)    : STORE_TYPE,
            (NiFiProperties.SECURITY_TRUSTSTORE)       : TRUSTSTORE_PATH,
            (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): STORE_PASSWORD,
            (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : STORE_TYPE,
    ]))

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        TestAppender.reset()
    }

    @AfterClass
    static void tearDownOnce() throws Exception {

    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {
        // Cleans up the EMH so it can be reinitialized when a new Jetty server starts
        ExtensionManagerHolder.INSTANCE = null
        TestAppender.reset()
    }

    @Test
    void testShouldDetectHttpAndHttpsConfigurationsBothPresent() {
        // Arrange
        Map badProps = [
                (NiFiProperties.WEB_HTTP_HOST) : "localhost",
                (NiFiProperties.WEB_HTTPS_HOST): "secure.host.com",
                (NiFiProperties.WEB_THREADS)   : NiFiProperties.DEFAULT_WEB_THREADS
        ]
        NiFiProperties mockProps = Mockito.mock(NiFiProperties.class)
        Mockito.when(mockProps.getPort()).thenReturn(DEFAULT_HTTP_PORT)
        Mockito.when(mockProps.getSslPort()).thenReturn(DEFAULT_HTTPS_PORT)

        Mockito.when(mockProps.getProperty(ArgumentMatchers.anyString())).thenAnswer(new Answer<Object>() {
            @Override
            Object answer(InvocationOnMock invocation) throws Throwable {
                badProps[(String) invocation.getArgument(0)] ?: "no_value"
            }
        })

        // Act
        boolean bothConfigsPresent = JettyServer.bothHttpAndHttpsConnectorsConfigured(mockProps)
        logger.info("Both configs present: ${bothConfigsPresent}")
        def log = TestAppender.getLogLines()

        // Assert
        assert bothConfigsPresent
        assert !log.isEmpty()
        assert log.first() =~ "Both the HTTP and HTTPS connectors are configured in nifi.properties. Only one of these connectors should be configured. See the NiFi Admin Guide for more details"
    }

    @Test
    void testDetectHttpAndHttpsConfigurationsShouldAllowEither() {
        // Arrange
        Map httpMap = [
                (NiFiProperties.WEB_HTTP_HOST) : "localhost",
                (NiFiProperties.WEB_HTTPS_HOST): null,
        ]
        NiFiProperties httpProps = [
                getPort    : { -> DEFAULT_HTTP_PORT },
                getSslPort : { -> null },
                getProperty: { String prop ->
                    String value = httpMap[prop] ?: "no_value"
                    logger.mock("getProperty(${prop}) -> ${value}")
                    value
                },
        ] as NiFiProperties

        Map httpsMap = [
                (NiFiProperties.WEB_HTTP_HOST) : null,
                (NiFiProperties.WEB_HTTPS_HOST): "secure.host.com",
        ]
        NiFiProperties httpsProps = [
                getPort    : { -> null },
                getSslPort : { -> DEFAULT_HTTPS_PORT },
                getProperty: { String prop ->
                    String value = httpsMap[prop] ?: "no_value"
                    logger.mock("getProperty(${prop}) -> ${value}")
                    value
                },
        ] as NiFiProperties

        // Act
        boolean bothConfigsPresentForHttp = JettyServer.bothHttpAndHttpsConnectorsConfigured(httpProps)
        logger.info("Both configs present for HTTP properties: ${bothConfigsPresentForHttp}")

        boolean bothConfigsPresentForHttps = JettyServer.bothHttpAndHttpsConnectorsConfigured(httpsProps)
        logger.info("Both configs present for HTTPS properties: ${bothConfigsPresentForHttps}")
        def log = TestAppender.getLogLines()

        // Assert
        assert !bothConfigsPresentForHttp
        assert !bothConfigsPresentForHttps

        // Verifies that the warning was not logged (messages are duplicated because of log4j.properties settings)
        assert log.size() == 4
        assert log.every { it =~ "Both configs present for HTTPS? properties: false" }
    }

    @Test
    void testShouldFailToStartWithHttpAndHttpsConfigurationsBothPresent() {
        // Arrange
        Map badProps = [
                (NiFiProperties.WEB_HTTP_HOST) : "localhost",
                (NiFiProperties.WEB_HTTPS_HOST): "secure.host.com",
        ]
        NiFiProperties mockProps = [
                getPort            : { -> DEFAULT_HTTP_PORT },
                getSslPort         : { -> DEFAULT_HTTPS_PORT },
                getProperty        : { String prop ->
                    String value = badProps[prop] ?: "no_value"
                    logger.mock("getProperty(${prop}) -> ${value}")
                    value
                },
                getWebThreads      : { -> NiFiProperties.DEFAULT_WEB_THREADS },
                getWebMaxHeaderSize: { -> NiFiProperties.DEFAULT_WEB_MAX_HEADER_SIZE },
                isHTTPSConfigured  : { -> true }
        ] as NiFiProperties

        // The web server should fail to start and exit Java
        exit.expectSystemExitWithStatus(1)
        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {
                final String standardErr = systemErrRule.getLog()
                List<String> errLines = standardErr.split("\n")

                assert errLines.any { it =~ "Failed to start web server: " }
                assert errLines.any { it =~ "Shutting down..." }
            }
        })

        // Act
        JettyServer jettyServer = new JettyServer()
        jettyServer.initialize(mockProps, null, [] as Set<Bundle>, null)
        
        // Assert

        // Assertions defined above
    }

    /**
     * Regression test added after NiFi 1.12.0 because Jetty upgrade to 9.4.26 no longer works
     * with multiple certificate keystores.
     */
    @Test
    void testShouldStartWithMultipleCertificatePKCS12Keystore() {
        // Arrange
        final String externalHostname = "localhost"

        NiFiProperties httpsProps = new NiFiProperties(new Properties([
                (NiFiProperties.WEB_HTTPS_PORT): HTTPS_PORT as String,
                (NiFiProperties.WEB_HTTPS_HOST): externalHostname,
                (NiFiProperties.SECURITY_KEYSTORE): "src/test/resources/multiple_cert_keystore.p12",
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD): "passwordpassword",
                (NiFiProperties.SECURITY_KEYSTORE_TYPE): "PKCS12",
                (NiFiProperties.NAR_LIBRARY_DIRECTORY): "target/"
        ]))

        JettyServer jetty = createJettyServer(httpsProps)
        Server internalServer = jetty.server
        List<Connector> connectors = Arrays.asList(internalServer.connectors)

        // Act
        jetty.start()

        // Assert
        assertServerConnector(connectors, externalHostname, HTTPS_PORT)

        // Clean up
        jetty.stop()
    }

    /**
     * Regression test added after NiFi 1.12.0 because Jetty upgrade to 9.4.26 no longer works
     * with multiple certificate keystores.
     */
    @Test
    void testShouldStartWithMultipleCertificateJKSKeystore() {
        // Arrange
        final String externalHostname = "localhost"

        NiFiProperties httpsProps = new NiFiProperties(new Properties([
                (NiFiProperties.WEB_HTTPS_PORT): HTTPS_PORT as String,
                (NiFiProperties.WEB_HTTPS_HOST): externalHostname,
                (NiFiProperties.SECURITY_KEYSTORE): "src/test/resources/multiple_cert_keystore.jks",
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD): "passwordpassword",
                (NiFiProperties.SECURITY_KEYSTORE_TYPE): "JKS",
                (NiFiProperties.NAR_LIBRARY_DIRECTORY): "target/"
        ]))

        JettyServer jetty = createJettyServer(httpsProps)
        Server internalServer = jetty.server
        List<Connector> connectors = Arrays.asList(internalServer.connectors)

        // Act
        jetty.start()

        // Assert
        assertServerConnector(connectors, externalHostname, HTTPS_PORT)

        // Clean up
        jetty.stop()
    }

    private static JettyServer createJettyServer(NiFiProperties httpsProps) {
        Server internalServer = new Server()
        JettyServer jetty = new JettyServer(internalServer, httpsProps)
        jetty.systemBundle = SystemBundle.create(httpsProps)
        jetty.bundles = [] as Set<Bundle>
        jetty.extensionMapping = [size: { -> 0 }] as ExtensionMapping
        jetty.configureHttpsConnector(internalServer, new HttpConfiguration())
        jetty
    }

    @Test
    void testShouldConfigureHTTPSConnector() {
        // Arrange
        final String externalHostname = "localhost"

        NiFiProperties httpsProps = new NiFiProperties(new Properties([
                (NiFiProperties.WEB_HTTPS_PORT): HTTPS_PORT as String,
                (NiFiProperties.WEB_HTTPS_HOST): externalHostname,
        ]))

        Server internalServer = new Server()
        JettyServer jetty = new JettyServer(internalServer, httpsProps)

        // Act
        jetty.configureHttpsConnector(internalServer, new HttpConfiguration())
        List<Connector> connectors = Arrays.asList(internalServer.connectors)

        // Assert
        assertServerConnector(connectors, externalHostname, HTTPS_PORT)
    }

    @Test
    void testShouldSupportTLSv1_3WhenProtocolFound() {
        // Arrange
        String[] defaultProtocols = SSLContext.getDefault().defaultSSLParameters.protocols
        Assume.assumeTrue("This test should only run when TLSv1.3 is found in the set of default protocols", defaultProtocols.contains(TLS_1_3_PROTOCOL))

        Server internalServer = new Server()
        JettyServer jetty = new JettyServer(internalServer, httpsProps)

        jetty.configureConnectors(internalServer)
        List<Connector> connectors = Arrays.asList(internalServer.connectors)
        internalServer.start()

        // Create a (client) socket which only supports TLSv1.3
        TlsConfiguration tls13ClientConf = StandardTlsConfiguration.fromNiFiProperties(httpsProps)
        SSLSocketFactory socketFactory = org.apache.nifi.security.util.SslContextFactory.createSSLSocketFactory(tls13ClientConf)

        SSLSocket socket = (SSLSocket) socketFactory.createSocket(HTTPS_HOSTNAME, HTTPS_PORT)
        socket.setEnabledProtocols([TLS_1_3_PROTOCOL] as String[])
        socket.setEnabledCipherSuites(TLS_1_3_CIPHER_SUITES as String[])

        // Act
        String response = makeTLSRequest(socket, "This is a TLS 1.3 request")

        // Assert
        assert response =~ "HTTP/1.1 400"

        assertServerConnector(connectors)

        // Clean up
        internalServer.stop()
    }

    @Test
    void testShouldNotSupportTLSv1_3WhenProtocolNotFound() {
        // Arrange
        Assume.assumeTrue("This test should only run when TLSv1.3 is not found in the set of default protocols", !TlsPlatform.supportedProtocols.contains(TLS_1_3_PROTOCOL))

        Server internalServer = new Server()
        JettyServer jetty = new JettyServer(internalServer, httpsProps)

        jetty.configureConnectors(internalServer)
        List<Connector> connectors = Arrays.asList(internalServer.connectors)
        internalServer.start()

        TlsConfiguration tlsConfiguration = StandardTlsConfiguration.fromNiFiProperties(httpsProps)

        // Create a "default" (client) socket (which supports TLSv1.2)
        SSLSocketFactory defaultSocketFactory = org.apache.nifi.security.util.SslContextFactory.createSSLSocketFactory(tlsConfiguration)
        SSLSocket defaultSocket = (SSLSocket) defaultSocketFactory.createSocket(HTTPS_HOSTNAME, HTTPS_PORT)

        // Act
        String tls12Response = makeTLSRequest(defaultSocket, "This is a default socket request")

        def msg = shouldFail() {
            // Create a (client) socket which only supports TLSv1.3
            SSLSocketFactory tls13SocketFactory = org.apache.nifi.security.util.SslContextFactory.createSSLSocketFactory(tlsConfiguration)

            SSLSocket tls13Socket = (SSLSocket) tls13SocketFactory.createSocket(HTTPS_HOSTNAME, HTTPS_PORT)
            tls13Socket.setEnabledProtocols([TLS_1_3_PROTOCOL] as String[])
            tls13Socket.setEnabledCipherSuites(TLS_1_3_CIPHER_SUITES as String[])

            makeTLSRequest(tls13Socket, "This is a TLSv1.3 socket request")
        }
        logger.expected(msg)

        // Assert
        assert tls12Response =~ "HTTP"

        assertServerConnector(connectors)

        // Clean up
        internalServer.stop()
    }

    /**
     * Returns the server's response body as a String. Closes the socket connection.
     *
     * @param socket
     * @param requestMessage
     * @return
     */
    private static String makeTLSRequest(Socket socket, String requestMessage) {
        InputStream socketInputStream = new BufferedInputStream(socket.getInputStream())
        OutputStream socketOutputStream = new BufferedOutputStream(socket.getOutputStream())

        socketOutputStream.write(requestMessage.getBytes())
        socketOutputStream.flush()

        byte[] data = new byte[2048]
        int len = socketInputStream.read(data)
        if (len <= 0) {
            throw new IOException("no data received")
        }
        final String trimmedResponse = new String(data, 0, len, StandardCharsets.UTF_8)
        logger.info("Client received ${len} bytes from server: \n${trimmedResponse}\n----End of response----")
        socket.close()
        trimmedResponse
    }

    private static void assertServerConnector(List<Connector> connectors,
                                              String EXPECTED_HOSTNAME = HTTPS_HOSTNAME,
                                              int EXPECTED_PORT = HTTPS_PORT) {
        // Assert the server connector is correct
        assert connectors.size() == 1
        ServerConnector connector = connectors.first() as ServerConnector
        assert connector.host == EXPECTED_HOSTNAME
        assert connector.port == EXPECTED_PORT
        assert connector.getProtocols() == ['ssl', 'http/1.1']

        SslConnectionFactory connectionFactory = connector.getConnectionFactory("ssl") as SslConnectionFactory
        SslContextFactory sslContextFactory = connectionFactory.getSslContextFactory()
        assert (sslContextFactory.getExcludeProtocols() as List<String>).containsAll(LEGACY_TLS_PROTOCOLS)
    }

    @Test
    void testShouldEnableContentLengthFilterIfWebMaxContentSizeSet() {
        // Arrange
        Map defaultProps = [
                (NiFiProperties.WEB_HTTP_PORT)       : DEFAULT_HTTP_PORT as String,
                (NiFiProperties.WEB_HTTP_HOST)       : "localhost",
                (NiFiProperties.WEB_MAX_CONTENT_SIZE): "1 MB",
        ]
        NiFiProperties mockProps = new NiFiProperties(new Properties(defaultProps))

        List<FilterHolder> filters = []
        def mockWebContext = [
                addFilter: { FilterHolder fh, String path, EnumSet<DispatcherType> d ->
                    logger.mock("Called addFilter(${fh.name}, ${path}, ${d})")
                    filters.add(fh)
                    fh
                }] as WebAppContext

        JettyServer jettyServer = new JettyServer(new Server(), mockProps)
        logger.info("Created JettyServer: ${jettyServer.dump()}")

        String path = "/mock"

        final int MAX_CONTENT_LENGTH_BYTES = DataUnit.parseDataSize(defaultProps[NiFiProperties.WEB_MAX_CONTENT_SIZE], DataUnit.B).intValue()

        // Act
        jettyServer.addDenialOfServiceFilters(path, mockWebContext, mockProps)

        // Assert
        assert filters.size() == 2
        def filterNames = filters*.name
        logger.info("Web API Context has ${filters.size()} filters: ${filterNames.join(", ")}".toString())
        assert filterNames.contains("DoSFilter")
        assert filterNames.contains("ContentLengthFilter")

        FilterHolder clfHolder = filters.find { it.name == "ContentLengthFilter" }
        String maxContentLength = clfHolder.getInitParameter("maxContentLength")
        assert maxContentLength == MAX_CONTENT_LENGTH_BYTES as String

        // Filter is not instantiated just by adding it
//        ContentLengthFilter clf = filters?.find { it.className == "ContentLengthFilter" }?.filter as ContentLengthFilter
//        assert clf.getMaxContentLength() == MAX_CONTENT_LENGTH_BYTES
    }

    @Test
    void testShouldNotEnableContentLengthFilterIfWebMaxContentSizeEmpty() {
        // Arrange
        Map defaultProps = [
                (NiFiProperties.WEB_HTTP_PORT): DEFAULT_HTTP_PORT as String,
                (NiFiProperties.WEB_HTTP_HOST): "localhost",
        ]
        NiFiProperties mockProps = new NiFiProperties(new Properties(defaultProps))

        List<FilterHolder> filters = []
        def mockWebContext = [
                addFilter: { FilterHolder fh, String path, EnumSet<DispatcherType> d ->
                    logger.mock("Called addFilter(${fh.name}, ${path}, ${d})")
                    filters.add(fh)
                    fh
                }] as WebAppContext

        JettyServer jettyServer = new JettyServer(new Server(), mockProps)
        logger.info("Created JettyServer: ${jettyServer.dump()}")

        String path = "/mock"

        // Act
        jettyServer.addDenialOfServiceFilters(path, mockWebContext, mockProps)

        // Assert
        assert filters.size() == 1
        def filterNames = filters*.name
        logger.info("Web API Context has ${filters.size()} filters: ${filterNames.join(", ")}".toString())
        assert filterNames.contains("DoSFilter")
        assert !filterNames.contains("ContentLengthFilter")
    }
}

class TestAppender extends AppenderSkeleton {
    static final List<LoggingEvent> events = new ArrayList<>()

    @Override
    protected void append(LoggingEvent e) {
        synchronized (events) {
            events.add(e)
        }
    }

    static void reset() {
        synchronized (events) {
            events.clear()
        }
    }

    @Override
    void close() {
    }

    @Override
    boolean requiresLayout() {
        return false
    }

    static List<String> getLogLines() {
        synchronized (events) {
            events.collect { LoggingEvent le -> le.getRenderedMessage() }
        }
    }
}
