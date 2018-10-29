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
package org.apache.nifi.nar;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class TestNarLoader {

    private NiFiProperties properties;
    private ExtensionMapping extensionMapping;

    private NarLoader narLoader;
    private NarClassLoaders narClassLoaders;
    private ExtensionManager extensionManager;

    @Before
    public void setup() throws IOException, ClassNotFoundException {
        // Create NiFiProperties
        final String propertiesFile = "./src/test/resources/conf/nifi.properties";
        properties = NiFiProperties.createBasicNiFiProperties(propertiesFile , Collections.emptyMap());

        // Unpack NARs
        final Bundle systemBundle = SystemBundle.create(properties);
        extensionMapping = NarUnpacker.unpackNars(properties, systemBundle);
        assertEquals(0, extensionMapping.getAllExtensionNames().size());

        // Initialize NarClassLoaders
        narClassLoaders = new NarClassLoaders();
        narClassLoaders.init(properties.getFrameworkWorkingDirectory(), properties.getExtensionsWorkingDirectory());

        extensionManager = new ExtensionManager();
        extensionManager.discoverExtensions(systemBundle, narClassLoaders.getBundles());

        // Should have Framework and Jetty NARs loaded here
        assertEquals(2, narClassLoaders.getBundles().size());

        // No extensions should be loaded yet
        assertEquals(0, extensionManager.getExtensions(Processor.class).size());
        assertEquals(0, extensionManager.getExtensions(ControllerService.class).size());
        assertEquals(0, extensionManager.getExtensions(ReportingTask.class).size());

        // Create class we are testing
        narLoader = new NarLoader(
                properties.getExtensionsWorkingDirectory(),
                properties.getComponentDocumentationWorkingDirectory(),
                narClassLoaders,
                extensionManager,
                extensionMapping,
                (bundles) -> {});
    }

    @Test
    public void testNarLoader() {

    }

}
