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
import org.apache.nifi.bundle.BundleDetails
import org.apache.nifi.properties.StandardNiFiProperties
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.eclipse.jetty.server.Connector
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.FilterHolder
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
    void testShouldNotEnableContentLengthFilterIfWebMaxContentSizeEmpty() {
        // Arrange
        Map defaultProps = [
                (NiFiProperties.WEB_HTTP_HOST)        : "localhost",
                (NiFiProperties.WEB_MAX_CONTENT_SIZE) : NiFiProperties.DEFAULT_WEB_THREADS,
                (NiFiProperties.NAR_LIBRARY_DIRECTORY): TEST_LIB_PATH,
                (NiFiProperties.NAR_WORKING_DIRECTORY): "./target/work/nar"
        ]
        NiFiProperties mockProps = new StandardNiFiProperties(new Properties(defaultProps))

        // Generate the empty WAR files in the proper location
        prepareWars(REQUIRED_WARS, mockProps.getNarLibraryDirectories().first().toAbsolutePath().toString())

        // Form the bundles
        Set<Bundle> narBundles = prepareWARBundles(mockProps)
        logger.info("Prepared ${narBundles.size()} required bundles: ${narBundles*.bundleDetails*.coordinate}")


        def log = TestAppender.getLogLines()

        // Act
        JettyServer jettyServer = new JettyServer(mockProps, narBundles)

        // Attempt to override WAR loading
//        JettyServer jettyServer = new JettyServer(mockProps, [] as Set<Bundle>) {
//            public JettyServer(NiFiProperties props, Set<Bundle> narBundles) {
//                logger.mock("Overriding normal JettyServer constructor")
//            }
//
//            private Handler loadInitialWars(final Set<Bundle> bundles) {
//                logger.mock("Overriding normal loadInitialWars() method")
//
//                ServletContextHandler context = new ServletContextHandler()
//                ServletHolder defaultServlet = new ServletHolder("default", DefaultServlet.class)
////                defaultServlet.setInitParameter("resourceBase",System.getProperty("user.dir"));
////                defaultServlet.setInitParameter("dirAllowed","true");
//                context.addServlet(defaultServlet, "/")
//                context
//            }
//        }

        // Attempt to bypass WAR loading
//        Server internalServer = new Server()
//        JettyServer jettyServer = new JettyServer(internalServer, mockProps)
        logger.info("Created JettyServer: ${jettyServer.dump()}")

        // Assert
        def filters = jettyServer.webApiContext.getServletHandler().getFilters() as List<FilterHolder>
        logger.info("Web API Context has ${filters.size()} filters: ${filters*.displayName.join(", ")}".toString())
        assert filters*.displayName.contains("ContentLengthFilter")

        // TODO: Check for log errors (TBA)
        assert !log.isEmpty()
        assert log.first() =~ "Both the HTTP and HTTPS connectors are configured in nifi.properties. Only one of these connectors should be configured. See the NiFi Admin Guide for more details"
    }

    private static void prepareWars(List<String> wars = REQUIRED_WARS, String extensionsPath = "target/lib") {
        // Create the directory
        def extensionsDir = new File(extensionsPath)
        extensionsDir.mkdirs()

        // For each required WAR
        wars.each { String warName ->
            // Mock WAR file
            def mockWarFile = new File(extensionsPath, warName)
            boolean createdFile = mockWarFile.createNewFile()
            if (!createdFile) {
                fail("Could not create ${mockWarFile.path}")
            } else {
                logger.info("Created empty WAR file at ${mockWarFile.path}")
            }
        }
    }

    private static Bundle buildBundle(String warName, File workingDirectory, ClassLoader classLoader = ClassLoader.getSystemClassLoader()) {
        BundleCoordinate coordinate = new BundleCoordinate("org.apache.nifi.test", warName, VERSION)
        BundleDetails bundleDetails = new BundleDetails.Builder()
                .coordinate(coordinate)
//                .dependencyCoordinate(DEPENDENCY_COORDINATE)
                .workingDir(workingDirectory)
                .buildBranch(BRANCH)
                .buildJdk(JDK)
                .buildRevision(REVISION)
                .buildTag(TAG)
                .buildTimestamp(TIMESTAMP)
                .builtBy(USER)
                .build()
        new Bundle(bundleDetails, classLoader)
    }

    /**
     * Returns a collection of {@link Bundle} objects required to start the NiFi {@link JettyServer}. Replicates the behavior of {@code new NiFi()}.
     *
     * @param mockProps the NiFi properties referencing a test resource directory containing stubbed WARs
     * @return the parsed Bundles
     */
    private static Set<Bundle> prepareWARBundles(StandardNiFiProperties mockProps) {
        logger.info("Setting up stubbed WAR bundles")
        logger.info("Current working directory: ${new File(".").absolutePath}")
        File fwd = mockProps.getFrameworkWorkingDirectory()
        logger.info("Framework working directory: ${fwd.absolutePath}")

        Set<Bundle> bundles = REQUIRED_WARS.collect { String warName ->
            def bundle = buildBundle(warName, fwd)
            logger.info("WAR: ${warName.padLeft(30, " ")} - built bundle: ${bundle}")
            bundle
        }

        bundles

//        def rootClassLoader = ClassLoader.getSystemClassLoader()
//        def systemBundle = SystemBundle.create(mockProps, rootClassLoader)
//        def extensionMapping = NarUnpacker.unpackNars(mockProps, systemBundle)
//        def narClassLoaders = NarClassLoadersHolder.getInstance()
//        narClassLoaders.init(rootClassLoader, mockProps.getFrameworkWorkingDirectory(), mockProps.getExtensionsWorkingDirectory())
//        def frameworkClassLoader = narClassLoaders.getFrameworkBundle().getClassLoader()
//        def narBundles = narClassLoaders.getBundles()
//
//        Bundle mockWebApiBundle = new Bundle()
//        Set<Bundle> mockBundles = []
//        narBundles
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