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

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTestNarLoader {
    abstract String getWorkDir();
    abstract String getNarAutoloadDir();
    abstract String getPropertiesFile();

    Bundle systemBundle;
    NiFiProperties properties;
    ExtensionMapping extensionMapping;

    StandardNarLoader narLoader;
    NarClassLoaders narClassLoaders;
    ExtensionDiscoveringManager extensionManager;

    @BeforeClass
    public static void setupClass() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void setup() throws IOException, ClassNotFoundException {
        deleteDir(getWorkDir());
        deleteDir(getNarAutoloadDir());

        final File extensionsDir = new File(getNarAutoloadDir());
        assertTrue(extensionsDir.mkdirs());

        // Create NiFiProperties
        final String propertiesFile = getPropertiesFile();
        properties = NiFiProperties.createBasicNiFiProperties(propertiesFile, Collections.emptyMap());

        // Unpack NARs
        systemBundle = SystemBundle.create(properties);
        extensionMapping = NarUnpacker.unpackNars(properties, systemBundle);
        assertEquals(0, extensionMapping.getAllExtensionNames().size());

        // Initialize NarClassLoaders
        narClassLoaders = new NarClassLoaders();
        narClassLoaders.init(properties.getFrameworkWorkingDirectory(), properties.getExtensionsWorkingDirectory());

        extensionManager = new StandardExtensionDiscoveringManager();

        // Should have Framework, Jetty, and NiFiServer NARs loaded here
        assertEquals(3, narClassLoaders.getBundles().size());

        // No extensions should be loaded yet
        assertEquals(0, extensionManager.getExtensions(Processor.class).size());
        assertEquals(0, extensionManager.getExtensions(ControllerService.class).size());
        assertEquals(0, extensionManager.getExtensions(ReportingTask.class).size());

        // Create class we are testing
        narLoader = new StandardNarLoader(
                properties.getExtensionsWorkingDirectory(),
                properties.getComponentDocumentationWorkingDirectory(),
                narClassLoaders,
                extensionManager,
                extensionMapping,
                (bundles) -> {
                });
    }

    private void deleteDir(String path) throws IOException {
        Path directory = Paths.get(path);
        if (!directory.toFile().exists()) {
            return;
        }

        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
