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
package org.apache.nifi.ssl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLContextServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(SSLContextServiceTest.class);
    private final String KEYSTORE_PATH = "src/test/resources/keystore.jks";
    private final String KEYSTORE_AND_TRUSTSTORE_PASSWORD = "passwordpassword";
    private final String JKS_TYPE = "JKS";
    private final String TRUSTSTORE_PATH = "src/test/resources/truststore.jks";
    private static final String KEYSTORE_WITH_KEY_PASSWORD_PATH = "src/test/resources/keystore-with-key-password.jks";

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder(new File("src/test/resources"));

    @Test
    public void testShouldFailToAddControllerServiceWithNoProperties() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<>();
        runner.addControllerService("test-no-properties", service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testShouldFailToAddControllerServiceWithoutKeystoreType() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), KEYSTORE_PATH);
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        runner.addControllerService("test-no-keystore-type", service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testShouldFailToAddControllerServiceWithOnlyTruststorePath() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), KEYSTORE_PATH);
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), JKS_TYPE);
        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), TRUSTSTORE_PATH);
        runner.addControllerService("test-no-truststore-password-or-type", service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testShouldFailToAddControllerServiceWithWrongPasswords() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), KEYSTORE_PATH);
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "wrongpassword");
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "PKCS12");
        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), TRUSTSTORE_PATH);
        properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "wrongpassword");
        properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), JKS_TYPE);
        runner.addControllerService("test-wrong-passwords", service, properties);

        runner.assertNotValid(service);
    }

    @Test
    public void testShouldFailToAddControllerServiceWithNonExistentFiles() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/DOES-NOT-EXIST.jks");
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "PKCS12");
        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), TRUSTSTORE_PATH);
        properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), JKS_TYPE);
        runner.addControllerService("test-keystore-file-does-not-exist", service, properties);
        runner.assertNotValid(service);
    }

    @Test
    public void testGood() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        SSLContextService service = new StandardSSLContextService();
        runner.addControllerService("test-good1", service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), KEYSTORE_PATH);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), JKS_TYPE);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), TRUSTSTORE_PATH);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), JKS_TYPE);
        runner.enableControllerService(service);

        runner.setProperty("SSL Context Svc ID", "test-good1");
        runner.assertValid(service);
        service = (SSLContextService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-good1");
        Assert.assertNotNull(service);
        SSLContextService sslService = service;
        sslService.createContext();
    }

    @Test
    public void testGoodWithEL() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        SSLContextService service = new StandardSSLContextService();
        runner.addControllerService("test-good1", service);
        runner.setVariable("keystore", KEYSTORE_PATH);
        runner.setVariable("truststore", TRUSTSTORE_PATH);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), "${keystore}");
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), JKS_TYPE);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), "${truststore}");
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), JKS_TYPE);
        runner.enableControllerService(service);

        runner.setProperty("SSL Context Svc ID", "test-good1");
        runner.assertValid(service);
        service = (SSLContextService) runner.getProcessContext().getControllerServiceLookup().getControllerService("test-good1");
        Assert.assertNotNull(service);
        SSLContextService sslService = service;
        sslService.createContext();
    }

    @Test
    public void testWithChanges() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        SSLContextService service = new StandardSSLContextService();
        runner.addControllerService("test-good1", service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), KEYSTORE_PATH);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), JKS_TYPE);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), TRUSTSTORE_PATH);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), JKS_TYPE);
        runner.enableControllerService(service);

        runner.setProperty("SSL Context Svc ID", "test-good1");
        runner.assertValid(service);

        runner.disableControllerService(service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/DOES-NOT-EXIST.jks");
        runner.assertNotValid(service);

        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), KEYSTORE_PATH);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "badpassword");
        runner.assertNotValid(service);

        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @Test
    public void testValidationResultsCacheShouldExpire() throws InitializationException, IOException {
        // Arrange

        // Copy the keystore and truststore to a tmp directory so the originals are not modified
        File originalKeystore = new File(KEYSTORE_PATH);
        File originalTruststore = new File(TRUSTSTORE_PATH);

        File tmpKeystore = tmp.newFile("keystore-tmp.jks");
        File tmpTruststore = tmp.newFile("truststore-tmp.jks");

        Files.copy(originalKeystore.toPath(), tmpKeystore.toPath(), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(originalTruststore.toPath(), tmpTruststore.toPath(), StandardCopyOption.REPLACE_EXISTING);

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        StandardSSLContextService service = new StandardSSLContextService();
        final String serviceIdentifier = "test-should-expire";
        runner.addControllerService(serviceIdentifier, service);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE.getName(), tmpKeystore.getAbsolutePath());
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        runner.setProperty(service, StandardSSLContextService.KEYSTORE_TYPE.getName(), JKS_TYPE);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE.getName(), tmpTruststore.getAbsolutePath());
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        runner.setProperty(service, StandardSSLContextService.TRUSTSTORE_TYPE.getName(), JKS_TYPE);
        runner.enableControllerService(service);

        runner.setProperty("SSL Context Svc ID", serviceIdentifier);
        runner.assertValid(service);

        // Act
        boolean isDeleted = tmpKeystore.delete();
        assert isDeleted;
        assert !tmpKeystore.exists();
        logger.info("Deleted keystore file");

        // Manually validate the service (expecting cached result to be returned)
        final MockProcessContext processContext = (MockProcessContext) runner.getProcessContext();
        // This service does not use the state manager or variable registry
        final ValidationContext validationContext = new MockValidationContext(processContext, null, null);

        // Even though the keystore file is no longer present, because no property changed, the cached result is still valid
        Collection<ValidationResult> validationResults = service.customValidate(validationContext);
        assertTrue("validation results is not empty", validationResults.isEmpty());
        logger.info("(1) StandardSSLContextService#customValidate() returned true even though the keystore file is no longer available");

        // Assert

        // Have to exhaust the cached result by checking n-1 more times
        for (int i = 2; i < service.getValidationCacheExpiration(); i++) {
            validationResults = service.customValidate(validationContext);
            assertTrue("validation results is not empty", validationResults.isEmpty());
            logger.info("(" + i + ") StandardSSLContextService#customValidate() returned true even though the keystore file is no longer available");
        }

        validationResults = service.customValidate(validationContext);
        assertFalse("validation results is empty", validationResults.isEmpty());
        logger.info("(" + service.getValidationCacheExpiration() + ") StandardSSLContextService#customValidate() returned false because the cache expired");
    }

    @Test
    public void testGoodTrustOnly() throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        SSLContextService service = new StandardSSLContextService();
        HashMap<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), TRUSTSTORE_PATH);
        properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), JKS_TYPE);
        runner.addControllerService("test-good2", service, properties);
        runner.enableControllerService(service);

        runner.setProperty("SSL Context Svc ID", "test-good2");
        runner.assertValid();
        Assert.assertNotNull(service);
        service.createContext();
    }

    @Test
    @Deprecated
    public void testGoodKeyOnly() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        SSLContextService service = new StandardSSLContextService();
        HashMap<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), KEYSTORE_PATH);
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), JKS_TYPE);
        runner.addControllerService("test-good3", service, properties);
        runner.enableControllerService(service);

        runner.setProperty("SSL Context Svc ID", "test-good3");
        runner.assertValid();
        Assert.assertNotNull(service);
        service.createContext();
    }

    /**
     * This test asserts that the keystore password and key password are different. This is only
     * true because they were explicitly set that way. Normal keystores that do not have passwords
     * set on individual keys will fail this test.
     */
    @Test
    public void testDifferentKeyPassword() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), KEYSTORE_WITH_KEY_PASSWORD_PATH);
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        properties.put(StandardSSLContextService.KEY_PASSWORD.getName(), "keypassword");
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), JKS_TYPE);
        properties.put(StandardSSLContextService.TRUSTSTORE.getName(), TRUSTSTORE_PATH);
        properties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        properties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), JKS_TYPE);
        runner.addControllerService("test-diff-keys", service, properties);
        runner.enableControllerService(service);

        runner.setProperty("SSL Context Svc ID", "test-diff-keys");
        runner.assertValid();
        Assert.assertNotNull(service);
        service.createContext();
    }

    /**
     * This test asserts that the keystore password and key password are different. This is only
     * true because they were explicitly set that way. Normal keystores that do not have passwords
     * set on individual keys will fail this test.
     */
    @Test
    public void testDifferentKeyPasswordWithoutSpecifyingKeyPassword() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final SSLContextService service = new StandardSSLContextService();
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardSSLContextService.KEYSTORE.getName(), KEYSTORE_WITH_KEY_PASSWORD_PATH);
        properties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), KEYSTORE_AND_TRUSTSTORE_PASSWORD);
        properties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), JKS_TYPE);
        runner.addControllerService("test-diff-keys", service, properties);

        // Assert the service is not valid due to an internal "cannot recover key" because the key password is missing
        runner.assertNotValid(service);
    }
}
