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
import org.apache.nifi.bundle.BundleCoordinate
import org.apache.nifi.processor.DataUnit
import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.eclipse.jetty.server.Connector
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.FilterHolder
import org.eclipse.jetty.webapp.WebAppContext
import org.junit.After
import org.junit.AfterClass
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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.servlet.DispatcherType
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

    private static final String TEST_LIB_PATH = "./target/lib"

    private static final List<String> REQUIRED_WARS = ["nifi-web-api",
                                                       "nifi-web-error",
                                                       "nifi-web-docs",
                                                       "nifi-web-content-viewer",
                                                       "nifi-web"]

    private static final String VERSION = "1.12.0-SNAPSHOT"
    private static final BundleCoordinate DEPENDENCY_COORDINATE = new BundleCoordinate("org.apache.nifi.test", "parent", VERSION)
    private static final String BRANCH = "branch_test"
    private static final String JDK = "11.0.6"
    private static final String REVISION = "revision_test"
    private static final String TAG = "tag_test"
    private static final String TIMESTAMP = new Date().toString()
    private static final String USER = "user_test"

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
        TestAppender.reset()

        // Clean up target/lib
        new File(TEST_LIB_PATH).deleteDir()
    }

    @Test
    void testShouldDetectHttpAndHttpsConfigurationsBothPresent() {
        // Arrange
        Map badProps = [
                (NiFiProperties.WEB_HTTP_HOST) : "localhost",
                (NiFiProperties.WEB_HTTPS_HOST): "secure.host.com",
                (NiFiProperties.WEB_THREADS)   : NiFiProperties.DEFAULT_WEB_THREADS
        ]
        NiFiProperties mockProps = [
                getPort    : { -> 8080 },
                getSslPort : { -> 8443 },
                getProperty: { String prop ->
                    String value = badProps[prop] ?: "no_value"
                    logger.mock("getProperty(${prop}) -> ${value}")
                    value
                },
        ] as StandardNiFiProperties

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
                getPort    : { -> 8080 },
                getSslPort : { -> null },
                getProperty: { String prop ->
                    String value = httpMap[prop] ?: "no_value"
                    logger.mock("getProperty(${prop}) -> ${value}")
                    value
                },
        ] as StandardNiFiProperties

        Map httpsMap = [
                (NiFiProperties.WEB_HTTP_HOST) : null,
                (NiFiProperties.WEB_HTTPS_HOST): "secure.host.com",
        ]
        NiFiProperties httpsProps = [
                getPort    : { -> null },
                getSslPort : { -> 8443 },
                getProperty: { String prop ->
                    String value = httpsMap[prop] ?: "no_value"
                    logger.mock("getProperty(${prop}) -> ${value}")
                    value
                },
        ] as StandardNiFiProperties

        // Act
        boolean bothConfigsPresentForHttp = JettyServer.bothHttpAndHttpsConnectorsConfigured(httpProps)
        logger.info("Both configs present for HTTP properties: ${bothConfigsPresentForHttp}")

        boolean bothConfigsPresentForHttps = JettyServer.bothHttpAndHttpsConnectorsConfigured(httpsProps)
        logger.info("Both configs present for HTTPS properties: ${bothConfigsPresentForHttps}")
        def log = TestAppender.getLogLines()

        // Assert
        assert !bothConfigsPresentForHttp
        assert !bothConfigsPresentForHttps

        // Verifies that the warning was not logged
        assert log.size() == 2
        assert log.first() == "Both configs present for HTTP properties: false"
        assert log.last() == "Both configs present for HTTPS properties: false"
    }

    @Test
    void testShouldFailToStartWithHttpAndHttpsConfigurationsBothPresent() {
        // Arrange
        Map badProps = [
                (NiFiProperties.WEB_HTTP_HOST) : "localhost",
                (NiFiProperties.WEB_HTTPS_HOST): "secure.host.com",
        ]
        NiFiProperties mockProps = [
                getPort            : { -> 8080 },
                getSslPort         : { -> 8443 },
                getProperty        : { String prop ->
                    String value = badProps[prop] ?: "no_value"
                    logger.mock("getProperty(${prop}) -> ${value}")
                    value
                },
                getWebThreads      : { -> NiFiProperties.DEFAULT_WEB_THREADS },
                getWebMaxHeaderSize: { -> NiFiProperties.DEFAULT_WEB_MAX_HEADER_SIZE },
                isHTTPSConfigured  : { -> true }
        ] as StandardNiFiProperties

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
        JettyServer jettyServer = new JettyServer(mockProps, [] as Set<Bundle>)

        // Assert

        // Assertions defined above
    }

    @Test
    void testShouldConfigureHTTPSConnector() {
        // Arrange
        NiFiProperties httpsProps = new StandardNiFiProperties(rawProperties: new Properties([
//               (NiFiProperties.WEB_HTTP_PORT): null,
//               (NiFiProperties.WEB_HTTP_HOST): null,
(NiFiProperties.WEB_HTTPS_PORT): "8443",
(NiFiProperties.WEB_HTTPS_HOST): "secure.host.com",
        ]))

        Server internalServer = new Server()
        JettyServer jetty = new JettyServer(internalServer, httpsProps)

        // Act
        jetty.configureHttpsConnector(internalServer, new HttpConfiguration())
        List<Connector> connectors = Arrays.asList(internalServer.connectors)

        // Assert
        assert connectors.size() == 1
        ServerConnector connector = connectors.first() as ServerConnector
        assert connector.host == "secure.host.com"
        assert connector.port == 8443
    }

    @Test
    void testShouldEnableContentLengthFilterIfWebMaxContentSizeSet() {
        // Arrange
        Map defaultProps = [
                (NiFiProperties.WEB_HTTP_PORT)       : "8080",
                (NiFiProperties.WEB_HTTP_HOST)       : "localhost",
                (NiFiProperties.WEB_MAX_CONTENT_SIZE): "1 MB",
        ]
        NiFiProperties mockProps = new StandardNiFiProperties(new Properties(defaultProps))

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
        jettyServer.addContentLengthFilters(path, mockWebContext, mockProps)

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
                (NiFiProperties.WEB_HTTP_PORT): "8080",
                (NiFiProperties.WEB_HTTP_HOST): "localhost",
        ]
        NiFiProperties mockProps = new StandardNiFiProperties(new Properties(defaultProps))

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
        jettyServer.addContentLengthFilters(path, mockWebContext, mockProps)

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