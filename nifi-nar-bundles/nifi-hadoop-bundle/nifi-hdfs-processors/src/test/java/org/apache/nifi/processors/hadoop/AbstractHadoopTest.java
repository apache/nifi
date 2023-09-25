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
package org.apache.nifi.processors.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.FileResourceReference;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.StandardResourceReferences;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractHadoopTest {

    private static Logger logger;

    private File temporaryFile;
    private KerberosProperties kerberosProperties;
    private NiFiProperties mockedProperties;

    @BeforeAll
    public static void setUpClass() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.hadoop", "debug");
        logger = LoggerFactory.getLogger(AbstractHadoopTest.class);
    }

    @BeforeEach
    public void setup() throws IOException {
        // needed for calls to UserGroupInformation.setConfiguration() to work when passing in
        // config with Kerberos authentication enabled
        System.setProperty("java.security.krb5.realm", "nifi.com");
        System.setProperty("java.security.krb5.kdc", "nifi.kdc");

        temporaryFile = File.createTempFile("hadoop-test", ".properties");

        // mock properties and return a temporary file for the kerberos configuration
        mockedProperties = mock(NiFiProperties.class);
        when(mockedProperties.getKerberosConfigurationFile()).thenReturn(temporaryFile);
        kerberosProperties = new KerberosProperties(temporaryFile);
    }

    @AfterEach
    public void cleanUp() {
        temporaryFile.delete();
    }

    @Test
    public void testErrorConditions() {
        SimpleHadoopProcessor processor = new SimpleHadoopProcessor(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(processor);
        Collection<ValidationResult> results;
        ProcessContext pc;

        results = new HashSet<>();
        runner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, "target/classes");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());

        results = new HashSet<>();
        runner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, "target/doesnotexist");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        assertEquals(1, results.size());
    }

    @Test
    public void testTimeoutDetection() {
        SimpleHadoopProcessor processor = new SimpleHadoopProcessor(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(processor);
        assertThrows(IOException.class, () -> {
            final File brokenCoreSite = new File("src/test/resources/core-site-broken.xml");
            final ResourceReference brokenCoreSiteReference = new FileResourceReference(brokenCoreSite);
            final ResourceReferences references = new StandardResourceReferences(Collections.singletonList(brokenCoreSiteReference));
            final List<String> locations = references.asLocations();
            processor.resetHDFSResources(locations, runner.getProcessContext());
        });
    }

    @Test
    public void testKerberosOptions() {
        SimpleHadoopProcessor processor = new SimpleHadoopProcessor(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(processor);
        // should be valid since no kerberos options specified
        runner.assertValid();
        // no longer valid since only the principal is provided
        runner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site-security.xml");
        runner.setProperty(kerberosProperties.getKerberosPrincipal(), "principal");
        runner.assertNotValid();
        // invalid since the keytab does not exist
        runner.setProperty(kerberosProperties.getKerberosKeytab(), "BAD_KEYTAB_PATH");
        runner.assertNotValid();
        // valid since keytab is now a valid file location
        runner.setProperty(kerberosProperties.getKerberosKeytab(), temporaryFile.getAbsolutePath());
        runner.assertValid();
    }

    @Test
    public void testKerberosOptionsWithEL() throws Exception {
        SimpleHadoopProcessor processor = new SimpleHadoopProcessor(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(processor);

        // initialize the runner with EL for the kerberos properties
        runner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, "${variableHadoopConfigResources}");
        runner.setProperty(kerberosProperties.getKerberosPrincipal(), "${variablePrincipal}");
        runner.setProperty(kerberosProperties.getKerberosKeytab(), "${variableKeytab}");

        // add variables for all the kerberos properties except for the keytab
        runner.setEnvironmentVariableValue("variableHadoopConfigResources", "src/test/resources/core-site-security.xml");
        runner.setEnvironmentVariableValue("variablePrincipal", "principal");
        // test that the config is not valid, since the EL for keytab will return nothing, no keytab
        runner.assertNotValid();

        // add variable for the keytab
        runner.setEnvironmentVariableValue("variableKeytab", temporaryFile.getAbsolutePath());
        // test that the config is valid
        runner.assertValid();
    }

    @Test
    public void testKerberosOptionsWithBadKerberosConfigFile() throws Exception {
        // invalid since the kerberos configuration was changed to a non-existent file
        kerberosProperties = new KerberosProperties(new File("BAD_KERBEROS_PATH"));

        SimpleHadoopProcessor processor = new SimpleHadoopProcessor(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.assertValid();

        runner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site-security.xml");
        runner.setProperty(kerberosProperties.getKerberosPrincipal(), "principal");
        runner.setProperty(kerberosProperties.getKerberosKeytab(), temporaryFile.getAbsolutePath());
        runner.assertNotValid();
    }

    @Test
    public void testCustomValidateWhenKerberosUserServiceProvided() throws InitializationException {
        final TestRunner runner = createTestRunnerWithKerberosEnabled();
        final KerberosUserService kerberosUserService = enableKerberosUserService(runner);
        runner.setProperty(AbstractHadoopProcessor.KERBEROS_USER_SERVICE, kerberosUserService.getIdentifier());
        runner.assertValid();
    }

    @Test
    public void testCustomValidateWhenKerberosUserServiceAndKerberosCredentialsService() throws InitializationException {
        final TestRunner runner = createTestRunnerWithKerberosEnabled();

        final KerberosUserService kerberosUserService = enableKerberosUserService(runner);
        runner.setProperty(AbstractHadoopProcessor.KERBEROS_USER_SERVICE, kerberosUserService.getIdentifier());
        runner.assertValid();

        final KerberosCredentialsService credentialsService = enabledKerberosCredentialsService(runner);
        runner.setProperty(AbstractHadoopProcessor.KERBEROS_CREDENTIALS_SERVICE, credentialsService.getIdentifier());
        runner.assertNotValid();

        runner.removeProperty(AbstractHadoopProcessor.KERBEROS_USER_SERVICE);
        runner.assertValid();
    }

    @Test
    public void testCustomValidateWhenKerberosUserServiceAndPrincipalAndKeytab() throws InitializationException {
        final TestRunner runner = createTestRunnerWithKerberosEnabled();

        final KerberosUserService kerberosUserService = enableKerberosUserService(runner);
        runner.setProperty(AbstractHadoopProcessor.KERBEROS_USER_SERVICE, kerberosUserService.getIdentifier());
        runner.assertValid();

        runner.setProperty(kerberosProperties.getKerberosPrincipal(), "principal1");
        runner.setProperty(kerberosProperties.getKerberosKeytab(), temporaryFile.getAbsolutePath());
        runner.assertNotValid();

        runner.removeProperty(AbstractHadoopProcessor.KERBEROS_USER_SERVICE);
        runner.assertValid();
    }

    @Test
    public void testCustomValidateWhenKerberosUserServiceAndPrincipalAndPassword() throws InitializationException {
        final TestRunner runner = createTestRunnerWithKerberosEnabled();

        final KerberosUserService kerberosUserService = enableKerberosUserService(runner);
        runner.setProperty(AbstractHadoopProcessor.KERBEROS_USER_SERVICE, kerberosUserService.getIdentifier());
        runner.assertValid();

        runner.setProperty(kerberosProperties.getKerberosPrincipal(), "principal1");
        runner.setProperty(kerberosProperties.getKerberosPassword(), "password");
        runner.assertNotValid();

        runner.removeProperty(AbstractHadoopProcessor.KERBEROS_USER_SERVICE);
        runner.assertValid();
    }

    @Test
    public void testLocalFileSystemInvalid() {
        final SimpleHadoopProcessor processor = new SimpleHadoopProcessor(kerberosProperties, true, true);
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site.xml");

        final MockProcessContext processContext = (MockProcessContext) runner.getProcessContext();
        final ValidationContext validationContext = new MockValidationContext(processContext);
        final Collection<ValidationResult> results = processor.customValidate(validationContext);
        final Optional<ValidationResult> optionalResult = results.stream()
                .filter(result -> result.getSubject().equals("Hadoop File System"))
                .findFirst();
        assertTrue(optionalResult.isPresent(), "Hadoop File System Validation Result not found");
        final ValidationResult result = optionalResult.get();
        assertFalse(result.isValid(), "Hadoop File System Valid");
    }

    @Test
    public void testDistributedFileSystemValid() {
        final SimpleHadoopProcessor processor = new SimpleHadoopProcessor(kerberosProperties, true, true);
        TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site-security.xml");
        runner.setProperty(kerberosProperties.getKerberosPrincipal(), "principal");
        runner.setProperty(kerberosProperties.getKerberosKeytab(), temporaryFile.getAbsolutePath());
        runner.assertValid();
    }

    @Test
    public void testGetNormalizedPathWithoutFileSystem() throws URISyntaxException {
        AbstractHadoopProcessor processor = initProcessorForTestGetNormalizedPath("abfs://container1@storageaccount1");
        TestRunner runner = initTestRunnerForTestGetNormalizedPath(processor, "/dir1");

        Path path = processor.getNormalizedPath(runner.getProcessContext(), AbstractHadoopProcessor.DIRECTORY);

        assertEquals("/dir1", path.toString());
        assertTrue(runner.getLogger().getWarnMessages().isEmpty());
    }

    @Test
    public void testGetNormalizedPathWithCorrectFileSystem() throws URISyntaxException {
        AbstractHadoopProcessor processor = initProcessorForTestGetNormalizedPath("abfs://container2@storageaccount2");
        TestRunner runner = initTestRunnerForTestGetNormalizedPath(processor, "abfs://container2@storageaccount2/dir2");

        Path path = processor.getNormalizedPath(runner.getProcessContext(), AbstractHadoopProcessor.DIRECTORY);

        assertEquals("/dir2", path.toString());
        assertTrue(runner.getLogger().getWarnMessages().isEmpty());
    }

    @Test
    public void testGetNormalizedPathWithIncorrectScheme() throws URISyntaxException {
        AbstractHadoopProcessor processor = initProcessorForTestGetNormalizedPath("abfs://container3@storageaccount3");
        TestRunner runner = initTestRunnerForTestGetNormalizedPath(processor, "hdfs://container3@storageaccount3/dir3");

        Path path = processor.getNormalizedPath(runner.getProcessContext(), AbstractHadoopProcessor.DIRECTORY);

        assertEquals("/dir3", path.toString());
        assertFalse(runner.getLogger().getWarnMessages().isEmpty());
    }

    @Test
    public void testGetNormalizedPathWithIncorrectAuthority() throws URISyntaxException {
        AbstractHadoopProcessor processor = initProcessorForTestGetNormalizedPath("abfs://container4@storageaccount4");
        TestRunner runner = initTestRunnerForTestGetNormalizedPath(processor, "abfs://container*@storageaccount*/dir4");

        Path path = processor.getNormalizedPath(runner.getProcessContext(), AbstractHadoopProcessor.DIRECTORY);

        assertEquals("/dir4", path.toString());
        assertFalse(runner.getLogger().getWarnMessages().isEmpty());
    }

    @Test
    public void testGetNormalizedPathWithoutAuthority() throws URISyntaxException {
        AbstractHadoopProcessor processor = initProcessorForTestGetNormalizedPath("hdfs://myhost:9000");
        TestRunner runner = initTestRunnerForTestGetNormalizedPath(processor, "hdfs:///dir5");

        Path path = processor.getNormalizedPath(runner.getProcessContext(), AbstractHadoopProcessor.DIRECTORY);

        assertEquals("/dir5", path.toString());
        assertTrue(runner.getLogger().getWarnMessages().isEmpty());
    }

    private AbstractHadoopProcessor initProcessorForTestGetNormalizedPath(String fileSystemUri) throws URISyntaxException {
        final FileSystem fileSystem = mock(FileSystem.class);
        when(fileSystem.getUri()).thenReturn(new URI(fileSystemUri));

        final PutHDFS processor = new PutHDFS() {
            @Override
            protected FileSystem getFileSystem() {
                return fileSystem;
            }
        };

        return processor;
    }

    private TestRunner initTestRunnerForTestGetNormalizedPath(AbstractHadoopProcessor processor, String directory) throws URISyntaxException {
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.setProperty(AbstractHadoopProcessor.DIRECTORY, directory);

        return runner;
    }

    private KerberosUserService enableKerberosUserService(final TestRunner runner) throws InitializationException {
        final KerberosUserService kerberosUserService = mock(KerberosUserService.class);
        when(kerberosUserService.getIdentifier()).thenReturn("userService1");
        runner.addControllerService(kerberosUserService.getIdentifier(), kerberosUserService);
        runner.enableControllerService(kerberosUserService);
        return kerberosUserService;
    }

    private KerberosCredentialsService enabledKerberosCredentialsService(final TestRunner runner) throws InitializationException {
        final KerberosCredentialsService credentialsService = mock(KerberosCredentialsService.class);
        when(credentialsService.getIdentifier()).thenReturn("credsService1");
        when(credentialsService.getPrincipal()).thenReturn("principal1");
        when(credentialsService.getKeytab()).thenReturn("keytab1");

        runner.addControllerService(credentialsService.getIdentifier(), credentialsService);
        runner.enableControllerService(credentialsService);
        return credentialsService;
    }

    private TestRunner createTestRunnerWithKerberosEnabled() {
        final SimpleHadoopProcessor processor = new SimpleHadoopProcessor(kerberosProperties);
        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.assertValid();

        runner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site-security.xml");
        runner.assertNotValid();
        return runner;
    }

}
