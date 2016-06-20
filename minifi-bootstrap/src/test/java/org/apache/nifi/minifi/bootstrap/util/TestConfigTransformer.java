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

package org.apache.nifi.minifi.bootstrap.util;

import org.apache.nifi.minifi.bootstrap.exception.InvalidConfigurationException;
import org.apache.nifi.minifi.commons.schema.exception.SchemaLoaderException;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestConfigTransformer {

    @Test
    public void doesTransformFile() throws Exception {
        ConfigTransformer.transformConfigFile("./src/test/resources/config.yml", "./target/");
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformInputStream() throws Exception {
        File inputFile = new File("./src/test/resources/config.yml");
        ConfigTransformer.transformConfigFile(new FileInputStream(inputFile), "./target/");

        File nifiPropertiesFile = new File("./target/nifi.properties");
        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnDefaultFile() throws Exception {
        ConfigTransformer.transformConfigFile("./src/test/resources/default.yml", "./target/");
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnMultipleProcessors() throws Exception {
        ConfigTransformer.transformConfigFile("./src/test/resources/config-multiple-processors.yml", "./target/");
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnMultipleRemoteProcessingGroups() throws Exception {
        ConfigTransformer.transformConfigFile("./src/test/resources/config-multiple-RPGs.yml", "./target/");
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnMultipleInputPorts() throws Exception {
        ConfigTransformer.transformConfigFile("./src/test/resources/config-multiple-input-ports.yml", "./target/");
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void doesTransformOnMinimal() throws Exception {
        ConfigTransformer.transformConfigFile("./src/test/resources/config-minimal.yml", "./target/");
        File nifiPropertiesFile = new File("./target/nifi.properties");

        assertTrue(nifiPropertiesFile.exists());
        assertTrue(nifiPropertiesFile.canRead());

        nifiPropertiesFile.deleteOnExit();

        File flowXml = new File("./target/flow.xml.gz");
        assertTrue(flowXml.exists());
        assertTrue(flowXml.canRead());

        flowXml.deleteOnExit();
    }

    @Test
    public void handleTransformInvalidFile() throws Exception {
        try {
            ConfigTransformer.transformConfigFile("./src/test/resources/config-invalid.yml", "./target/");
            fail("Invalid configuration file was not detected.");
        } catch (SchemaLoaderException e){
            assertEquals("Provided YAML configuration is not a Map", e.getMessage());
        }
    }

    @Test
    public void handleTransformMalformedField() throws Exception {
        try {
            ConfigTransformer.transformConfigFile("./src/test/resources/config-malformed-field.yml", "./target/");
            fail("Invalid configuration file was not detected.");
        } catch (InvalidConfigurationException e){
            assertEquals("Failed to transform config file due to:['threshold' in section 'Swap' because it is found but could not be parsed as a Number]", e.getMessage());
        }
    }

    @Test
    public void handleTransformEmptyFile() throws Exception {
        try {
            ConfigTransformer.transformConfigFile("./src/test/resources/config-empty.yml", "./target/");
            fail("Invalid configuration file was not detected.");
        } catch (SchemaLoaderException e){
            assertEquals("Provided YAML configuration is not a Map", e.getMessage());
        }
    }

    @Test
    public void handleTransformFileMissingRequiredField() throws Exception {
        try {
            ConfigTransformer.transformConfigFile("./src/test/resources/config-missing-required-field.yml", "./target/");
            fail("Invalid configuration file was not detected.");
        } catch (InvalidConfigurationException e){
            assertEquals("Failed to transform config file due to:['class' in section 'Processors' because it was not found and it is required]", e.getMessage());
        }
    }

    @Test
    public void handleTransformFileMultipleProblems() throws Exception {
        try {
            ConfigTransformer.transformConfigFile("./src/test/resources/config-multiple-problems.yml", "./target/");
            fail("Invalid configuration file was not detected.");
        } catch (InvalidConfigurationException e){
            assertEquals("Failed to transform config file due to:['scheduling strategy' in section 'Provenance Reporting' because it is not a valid scheduling strategy], ['class' in section " +
                    "'Processors' because it was not found and it is required], ['source name' in section 'Connections' because it was not found and it is required]", e.getMessage());
        }
    }
}
