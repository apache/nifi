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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockValidationContext;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractHadoopTest {

    private static Logger logger;

    private File temporaryFile;
    private KerberosProperties kerberosProperties;
    private NiFiProperties mockedProperties;

    @BeforeClass
    public static void setUpClass() throws IOException {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.hadoop", "debug");
        logger = LoggerFactory.getLogger(AbstractHadoopTest.class);
    }

    @Before
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

    @After
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
        Assert.assertEquals(1, results.size());

        results = new HashSet<>();
        runner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, "target/doesnotexist");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void testTimeoutDetection() throws Exception {
        SimpleHadoopProcessor processor = new SimpleHadoopProcessor(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(processor);
        try {
            final File brokenCoreSite = new File("src/test/resources/core-site-broken.xml");
            final ResourceReference brokenCoreSiteReference = new FileResourceReference(brokenCoreSite);
            final ResourceReferences references = new StandardResourceReferences(Collections.singletonList(brokenCoreSiteReference));
            processor.resetHDFSResources(references, runner.getProcessContext());
            Assert.fail("Should have thrown SocketTimeoutException");
        } catch (IOException e) {
        }
    }

    @Test
    public void testKerberosOptions() throws Exception {
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
        runner.setVariable("variableHadoopConfigResources", "src/test/resources/core-site-security.xml");
        runner.setVariable("variablePrincipal", "principal");
        // test that the config is not valid, since the EL for keytab will return nothing, no keytab
        runner.assertNotValid();

        // add variable for the keytab
        runner.setVariable("variableKeytab", temporaryFile.getAbsolutePath());
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
        assertTrue("Hadoop File System Validation Result not found", optionalResult.isPresent());
        final ValidationResult result = optionalResult.get();
        assertFalse("Hadoop File System Valid", result.isValid());
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
    public void testGetNormalizedPathWithIncorrectFileSystem() throws URISyntaxException {
        AbstractHadoopProcessor processor = initProcessorForTestGetNormalizedPath("abfs://container3@storageaccount3");
        TestRunner runner = initTestRunnerForTestGetNormalizedPath(processor, "abfs://container*@storageaccount*/dir3");

        Path path = processor.getNormalizedPath(runner.getProcessContext(), AbstractHadoopProcessor.DIRECTORY);

        assertEquals("/dir3", path.toString());
        assertFalse(runner.getLogger().getWarnMessages().isEmpty());
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
}
