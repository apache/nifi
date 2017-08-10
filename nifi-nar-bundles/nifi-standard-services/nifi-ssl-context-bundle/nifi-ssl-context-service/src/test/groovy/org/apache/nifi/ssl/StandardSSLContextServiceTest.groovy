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

import org.apache.nifi.components.ValidationContext
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.components.Validator
import org.apache.nifi.state.MockStateManager
import org.apache.nifi.util.MockProcessContext
import org.apache.nifi.util.MockValidationContext
import org.apache.nifi.util.MockVariableRegistry
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
class StandardSSLContextServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(StandardSSLContextServiceTest.class)

    private static final String KEYSTORE_PATH = "src/test/resources/localhost-ks.jks"
    private static final String TRUSTSTORE_PATH = "src/test/resources/localhost-ts.jks"
    private static final String TRUSTSTORE_PATH_WITH_EL = "\${someAttribute}/localhost-ts.jks"

    private static final String KEYSTORE_PASSWORD = "localtest"
    private static final String TRUSTSTORE_PASSWORD = "localtest"

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
        assert msg =~ "invalid because Cannot access file"
        runner.assertNotValid(sslContextService)
    }

    @Test
    void testShouldNotEvaluateExpressionLanguageInFileValidator() {
        // Arrange
        final String VALID_TRUSTSTORE_PATH_WITH_EL = "\${literal(''):trim()}${TRUSTSTORE_PATH}"

        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class)
        String controllerServiceId = "ssl-context"
        final SSLContextService sslContextService = new StandardSSLContextService()
        runner.addControllerService(controllerServiceId, sslContextService)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE, VALID_TRUSTSTORE_PATH_WITH_EL)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD)
        runner.setProperty(sslContextService, StandardSSLContextService.TRUSTSTORE_TYPE, TRUSTSTORE_TYPE)

        // The verifySslConfig and customValidate methods correctly do not evaluate EL, but the custom file validator does, so extract it alone and validate
        Validator fileValidator = StandardSSLContextService.createFileExistsAndReadableValidator()
        final ValidationContext mockValidationContext = new MockValidationContext(
                runner.getProcessContext() as MockProcessContext,
                new MockStateManager(sslContextService),
                new MockVariableRegistry())

        // Act
        ValidationResult vr = fileValidator.validate(StandardSSLContextService.TRUSTSTORE.name, VALID_TRUSTSTORE_PATH_WITH_EL, mockValidationContext)
        logger.info("Custom file validation result: ${vr}")

        // Assert
        final MockProcessContext processContext = (MockProcessContext) runner.getProcessContext()

        // If the EL was evaluated, the paths would be identical
        assert processContext.getControllerServiceProperties(sslContextService).get(StandardSSLContextService.TRUSTSTORE, "") != TRUSTSTORE_PATH

        // If the EL was evaluated, the path would be valid
        assert !vr.isValid()
    }
}
