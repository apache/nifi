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
package org.apache.nifi.documentation;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DocGeneratorTest {

    @Test
    public void testProcessorLoadsNarResources() throws IOException, ClassNotFoundException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();

        NiFiProperties properties = loadSpecifiedProperties("/conf/nifi.properties");
        properties.setProperty(NiFiProperties.COMPONENT_DOCS_DIRECTORY, temporaryFolder.getRoot().getAbsolutePath());

        NarUnpacker.unpackNars(properties);

        NarClassLoaders.load(properties);

        ExtensionManager.discoverExtensions();

        DocGenerator.generate(properties);

        File processorDirectory = new File(temporaryFolder.getRoot(), "org.apache.nifi.processors.WriteResourceToStream");
        File indexHtml = new File(processorDirectory, "index.html");
        Assert.assertTrue(indexHtml + " should have been generated", indexHtml.exists());
        String generatedHtml = FileUtils.readFileToString(indexHtml);
        Assert.assertNotNull(generatedHtml);
        Assert.assertTrue(generatedHtml.contains("This example processor loads a resource from the nar and writes it to the FlowFile content"));
        Assert.assertTrue(generatedHtml.contains("files that were successfully processed"));
        Assert.assertTrue(generatedHtml.contains("files that were not successfully processed"));
        Assert.assertTrue(generatedHtml.contains("resources"));
    }

    private NiFiProperties loadSpecifiedProperties(String propertiesFile) {
        String file = DocGeneratorTest.class.getResource(propertiesFile).getFile();

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, file);

        NiFiProperties properties = NiFiProperties.getInstance();

        // clear out existing properties
        for (String prop : properties.stringPropertyNames()) {
            properties.remove(prop);
        }

        InputStream inStream = null;
        try {
            inStream = new BufferedInputStream(new FileInputStream(file));
            properties.load(inStream);
        } catch (final Exception ex) {
            throw new RuntimeException("Cannot load properties file due to "
                    + ex.getLocalizedMessage(), ex);
        } finally {
            if (null != inStream) {
                try {
                    inStream.close();
                } catch (final Exception ex) {
                    /**
                     * do nothing *
                     */
                }
            }
        }

        return properties;
    }
}
