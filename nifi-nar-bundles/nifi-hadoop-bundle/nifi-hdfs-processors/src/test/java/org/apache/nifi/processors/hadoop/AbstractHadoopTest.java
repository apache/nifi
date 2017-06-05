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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockProcessContext;
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
import java.util.Collection;
import java.util.HashSet;

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
        for (ValidationResult vr : results) {
            Assert.assertTrue(vr.toString().contains("is invalid because File target" + File.separator + "classes does not exist or is not a file"));
        }

        results = new HashSet<>();
        runner.setProperty(AbstractHadoopProcessor.HADOOP_CONFIGURATION_RESOURCES, "target/doesnotexist");
        runner.enqueue(new byte[0]);
        pc = runner.getProcessContext();
        if (pc instanceof MockProcessContext) {
            results = ((MockProcessContext) pc).validate();
        }
        Assert.assertEquals(1, results.size());
        for (ValidationResult vr : results) {
            Assert.assertTrue(vr.toString().contains("is invalid because File target" + File.separator + "doesnotexist does not exist or is not a file"));
        }
    }

    @Test
    public void testTimeoutDetection() throws Exception {
        SimpleHadoopProcessor processor = new SimpleHadoopProcessor(kerberosProperties);
        TestRunner runner = TestRunners.newTestRunner(processor);
        try {
            processor.resetHDFSResources("src/test/resources/core-site-broken.xml", runner.getProcessContext());
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
        runner.setProperty(AbstractHadoopProcessor.KERBEROS_RELOGIN_PERIOD, "${variableReloginPeriod}");
        runner.setProperty(kerberosProperties.getKerberosKeytab(), "${variableKeytab}");

        // add variables for all the kerberos properties except for the keytab
        runner.setVariable("variableHadoopConfigResources", "src/test/resources/core-site-security.xml");
        runner.setVariable("variablePrincipal", "principal");
        runner.setVariable("variableReloginPeriod", "4m");
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
}
