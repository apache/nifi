/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.kite;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.After;
import org.junit.rules.TemporaryFolder;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.Dataset;

public class TestConfigurationProperty {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    public File confLocation;

    private String datasetUri = null;
    private Dataset<Record> dataset = null;

    @BeforeClass
    public static void setUpSuite() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void saveConfiguration() throws IOException {
        Configuration conf = new Configuration(false);
        conf.setBoolean("nifi.config.canary", true);

        confLocation = temp.newFile("nifi-conf.xml");
        FileOutputStream out = new FileOutputStream(confLocation);
        conf.writeXml(out);
        out.close();
    }

    @Before
    public void createDataset() throws Exception {
        DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
                .schema(TestUtil.USER_SCHEMA)
                .build();
        this.datasetUri = "dataset:file:" + temp.newFolder("ns", "temp").toString();
        this.dataset = Datasets.create(datasetUri, descriptor, Record.class);
    }

    @After
    public void deleteDataset() throws Exception {
        Datasets.delete(datasetUri);
    }

    @Test
    public void testConfigurationCanary() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(StoreInKiteDataset.class);
        runner.setProperty(
                AbstractKiteProcessor.CONF_XML_FILES, confLocation.toString());

        Assert.assertFalse("Should not contain canary value",
                DefaultConfiguration.get().getBoolean("nifi.config.canary", false));

        AbstractKiteProcessor processor = new StoreInKiteDataset();
        ProcessContext context = runner.getProcessContext();
        processor.setDefaultConfiguration(context);

        Assert.assertTrue("Should contain canary value",
                DefaultConfiguration.get().getBoolean("nifi.config.canary", false));
    }

    @Test
    public void testFilesMustExist() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(StoreInKiteDataset.class);
        runner.setProperty(
                AbstractKiteProcessor.CONF_XML_FILES, temp.newFile().toString());
        runner.assertNotValid();
    }

    @Test
    public void testConfigurationExpressionLanguage() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(StoreInKiteDataset.class);
        runner.setProperty(
                AbstractKiteProcessor.CONF_XML_FILES, "${filename:substring(0,0):append('pom.xml')}");
        runner.setProperty(
                StoreInKiteDataset.KITE_DATASET_URI, datasetUri);
        runner.assertValid();
        // botch the Expression Language evaluation
        runner.setProperty(
                AbstractKiteProcessor.CONF_XML_FILES, "${filename:substring(0,0):");
        runner.assertNotValid();
    }
}
