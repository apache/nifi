/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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
package org.apache.nifi.ssl


import org.apache.nifi.security.util.ClientAuth
import org.apache.nifi.util.MockProcessContext
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.*
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.SSLContext
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
class StandardSSLContextServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(StandardSSLContextServiceTest.class)

    private static final String KEYSTORE_PATH = "src/test/resources/keystore.jks"
    private static final String TRUSTSTORE_PATH = "src/test/resources/truststore.jks"
    private static final String NO_PASSWORD_TRUSTSTORE_PATH = "src/test/resources/no-password-truststore.jks"
    private static final String TRUSTSTORE_PATH_WITH_EL = "\${someAttribute}/truststore.jks"

    private static final String KEYSTORE_PASSWORD = "passwordpassword"
    private static final String TRUSTSTORE_PASSWORD = "passwordpassword"
    private static final String TRUSTSTORE_NO_PASSWORD = ""

    private static final String KEYSTORE_TYPE = "JKS"
    private static final String TRUSTSTORE_TYPE = "JKS"

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder(new File("src/test/resources"))

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    @AfterClass
    static void tearDownOnce() throws Exception {
    }

    @Test
    void testShouldValidateSimpleFileValidatorPath() {
        // Arrange
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class)
        String controllerServiceId = "ssl-context"
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService(controllerServiceId, sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, TRUSTSTORE_PATH)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, TRUSTSTORE_TYPE)
        runner.enableControllerService(sslContextService)

        // Act
        runner.assertValid(sslContextService)

        // Assert
        final MockProcessContext processContext = (MockProcessContext) runner.getProcessContext()
        assert processContext.getControllerServiceProperties(sslContextService).get(StandardSSLContextService.TRUSTSTORE, "") == TRUSTSTORE_PATH
    }

    @Test
    void testTruststoreWithNoPasswordIsValid() {
        // Arrange
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class)
        String controllerServiceId = "ssl-context"
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService(controllerServiceId, sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, NO_PASSWORD_TRUSTSTORE_PATH)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, TRUSTSTORE_NO_PASSWORD)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, TRUSTSTORE_TYPE)
        runner.enableControllerService(sslContextService)

        // Act
        runner.assertValid(sslContextService)

        // Assert
        final MockProcessContext processContext = (MockProcessContext) runner.getProcessContext()
        assert processContext.getControllerServiceProperties(sslContextService).get(StandardSSLContextService.TRUSTSTORE, "") == NO_PASSWORD_TRUSTSTORE_PATH
    }

    @Test
    void testTruststoreWithNullPasswordIsValid() {
        // Arrange
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class)
        String controllerServiceId = "ssl-context"
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService(controllerServiceId, sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, NO_PASSWORD_TRUSTSTORE_PATH)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, null as String)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, TRUSTSTORE_TYPE)
        runner.enableControllerService(sslContextService)

        // Act
        runner.assertValid(sslContextService)

        // Assert
        final MockProcessContext processContext = (MockProcessContext) runner.getProcessContext()
        assert processContext.getControllerServiceProperties(sslContextService).get(StandardSSLContextService.TRUSTSTORE, "") == NO_PASSWORD_TRUSTSTORE_PATH
    }

    @Test
    void testTruststoreWithMissingPasswordIsValid() {
        // Arrange
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class)
        String controllerServiceId = "ssl-context"
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService(controllerServiceId, sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, NO_PASSWORD_TRUSTSTORE_PATH)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, TRUSTSTORE_TYPE)
        runner.enableControllerService(sslContextService)

        // Act
        runner.assertValid(sslContextService)

        // Assert
        final MockProcessContext processContext = (MockProcessContext) runner.getProcessContext()
        assert processContext.getControllerServiceProperties(sslContextService).get(StandardSSLContextService.TRUSTSTORE, "") == NO_PASSWORD_TRUSTSTORE_PATH
    }

    @Test
    void testShouldConnectWithPasswordlessTruststore() {
        // Arrange
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class)
        String controllerServiceId = "ssl-context"
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService(controllerServiceId, sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, NO_PASSWORD_TRUSTSTORE_PATH)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, TRUSTSTORE_TYPE)
        runner.enableControllerService(sslContextService)
        runner.assertValid(sslContextService)

        // Act
        SSLContext sslContext = sslContextService.createContext();

        // Assert
        assert sslContext
    }

    @Test
    void testShouldConnectWithPasswordlessTruststoreWhenKeystorePresent() {
        // Arrange
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class)
        String controllerServiceId = "ssl-context"
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService(controllerServiceId, sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE, KEYSTORE_PATH)
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_PASSWORD, KEYSTORE_PASSWORD)
        runner.setProperty(sslContextService, StandardSSLContextService.KEYSTORE_TYPE, KEYSTORE_TYPE)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, NO_PASSWORD_TRUSTSTORE_PATH)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, TRUSTSTORE_TYPE)
        runner.enableControllerService(sslContextService)
        runner.assertValid(sslContextService)

        // Act
        SSLContext sslContext = sslContextService.createContext();

        // Assert
        assert sslContext
    }

    @Test
    void testShouldNotValidateExpressionLanguageInFileValidator() {
        // Arrange
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class)
        String controllerServiceId = "ssl-context"
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService(controllerServiceId, sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, TRUSTSTORE_PATH_WITH_EL)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, TRUSTSTORE_TYPE)

        // Act
        def msg = shouldFail {
            runner.enableControllerService(sslContextService)
        }

        // Assert
        runner.assertNotValid(sslContextService)
    }

    /**
     * This test ensures that the deprecated ClientAuth enum is correctly mapped to the canonical enum.
     */
    @Test
    void testShouldTranslateValidDeprecatedClientAuths() {
        // Arrange
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class)
        String controllerServiceId = "ssl-context"
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService(controllerServiceId, sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, NO_PASSWORD_TRUSTSTORE_PATH)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, TRUSTSTORE_TYPE)
        runner.enableControllerService(sslContextService)
        runner.assertValid(sslContextService)

        // Act
        Map<SSLContextService.ClientAuth, SSLContext> sslContexts = SSLContextService.ClientAuth.values().collectEntries {  ca ->
            [ca, sslContextService.createSSLContext(ca)]
        }

        // Assert
        assert sslContexts.size() == ClientAuth.values().size()
        sslContexts.every {  clientAuth, sslContext ->
            assert ClientAuth.isValidClientAuthType(clientAuth.name())
            assert sslContext
        }
    }
}
