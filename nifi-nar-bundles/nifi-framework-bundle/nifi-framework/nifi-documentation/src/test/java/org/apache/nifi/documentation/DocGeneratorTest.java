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

import org.apache.commons.io.FileUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.ExtensionMapping;
import org.apache.nifi.nar.NarClassLoaders;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

public class DocGeneratorTest {

    @Test
    public void testProcessorLoadsNarResources() throws IOException, ClassNotFoundException {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();

        NiFiProperties properties = loadSpecifiedProperties("/conf/nifi.properties",
                NiFiProperties.COMPONENT_DOCS_DIRECTORY,
                temporaryFolder.getRoot().getAbsolutePath());

        final Bundle systemBundle = SystemBundle.create(properties);
        final ExtensionMapping mapping = NarUnpacker.unpackNars(properties, systemBundle);

        NarClassLoaders.getInstance().init(properties.getFrameworkWorkingDirectory(), properties.getExtensionsWorkingDirectory());

        ExtensionManager.discoverExtensions(systemBundle, NarClassLoaders.getInstance().getBundles());

        DocGenerator.generate(properties, mapping);

        final String extensionClassName = "org.apache.nifi.processors.WriteResourceToStream";
        final BundleCoordinate coordinate = mapping.getProcessorNames().get(extensionClassName).stream().findFirst().get();
        final String path = coordinate.getGroup() + "/" + coordinate.getId() + "/" + coordinate.getVersion() + "/" + extensionClassName;
        File processorDirectory = new File(temporaryFolder.getRoot(), path);
        File indexHtml = new File(processorDirectory, "index.html");
        Assert.assertTrue(indexHtml + " should have been generated", indexHtml.exists());
        String generatedHtml = FileUtils.readFileToString(indexHtml);
        Assert.assertNotNull(generatedHtml);
        Assert.assertTrue(generatedHtml.contains("This example processor loads a resource from the nar and writes it to the FlowFile content"));
        Assert.assertTrue(generatedHtml.contains("files that were successfully processed"));
        Assert.assertTrue(generatedHtml.contains("files that were not successfully processed"));
        Assert.assertTrue(generatedHtml.contains("resources"));
    }

    private NiFiProperties loadSpecifiedProperties(final String propertiesFile, final String key, final String value) {
        String file = DocGeneratorTest.class.getResource(propertiesFile).getFile();

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, file);

        final Properties props = new Properties();
        InputStream inStream = null;
        try {
            inStream = new BufferedInputStream(new FileInputStream(file));
            props.load(inStream);
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

        if (key != null && value != null) {
            props.setProperty(key, value);
        }

        return new NiFiProperties() {
            @Override
            public String getProperty(String key) {
                return props.getProperty(key);
            }

            @Override
            public Set<String> getPropertyKeys() {
                return props.stringPropertyNames();
            }
        };
    }
}
