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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestNarLoader {

    static final String WORK_DIR = "./target/work";
    static final String NAR_AUTOLOAD_DIR = "./target/extensions";
    static final String EXTENSIONS_DIR = "./src/test/resources/extensions";

    private NiFiProperties properties;
    private ExtensionMapping extensionMapping;

    private StandardNarLoader narLoader;
    private NarClassLoaders narClassLoaders;
    private ExtensionDiscoveringManager extensionManager;

    @Before
    public void setup() throws IOException, ClassNotFoundException {
        deleteDir(WORK_DIR);
        deleteDir(NAR_AUTOLOAD_DIR);

        final File extensionsDir = new File(NAR_AUTOLOAD_DIR);
        assertTrue(extensionsDir.mkdirs());

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

        extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(systemBundle, narClassLoaders.getBundles());

        // Should have Framework and Jetty NARs loaded here
        assertEquals(2, narClassLoaders.getBundles().size());

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
                (bundles) -> {});
    }

    @Test
    public void testNarLoaderWhenAllAvailable() throws IOException {
        // Copy all NARs from src/test/resources/extensions to target/extensions
        final File extensionsDir = new File(EXTENSIONS_DIR);
        final Path narAutoLoadDir = Paths.get(NAR_AUTOLOAD_DIR);
        for (final File extensionFile : extensionsDir.listFiles()) {
            Files.copy(extensionFile.toPath(), narAutoLoadDir.resolve(extensionFile.getName()), StandardCopyOption.REPLACE_EXISTING);
        }

        final List<File> narFiles = Arrays.asList(narAutoLoadDir.toFile().listFiles());
        assertEquals(3, narFiles.size());

        final NarLoadResult narLoadResult = narLoader.load(narFiles);
        assertNotNull(narLoadResult);
        assertEquals(3, narLoadResult.getLoadedBundles().size());
        assertEquals(0, narLoadResult.getSkippedBundles().size());

        assertEquals(5, narClassLoaders.getBundles().size());
        assertEquals(1, extensionManager.getExtensions(Processor.class).size());
        assertEquals(1, extensionManager.getExtensions(ControllerService.class).size());
        assertEquals(0, extensionManager.getExtensions(ReportingTask.class).size());
    }

    @Test
    public void testNarLoaderWhenDependentNarsAreMissing() throws IOException {
        final File extensionsDir = new File(EXTENSIONS_DIR);
        final Path narAutoLoadDir = Paths.get(NAR_AUTOLOAD_DIR);

        // Copy processors NAR first which depends on service API NAR
        final File processorsNar = new File(extensionsDir, "nifi-example-processors-nar-1.0.nar");
        final File targetProcessorNar = new File(narAutoLoadDir.toFile(), processorsNar.getName());
        Files.copy(processorsNar.toPath(), targetProcessorNar.toPath(), StandardCopyOption.REPLACE_EXISTING);

        // Attempt to load while only processor NAR is available
        final List<File> narFiles1 = Arrays.asList(targetProcessorNar);
        final NarLoadResult narLoadResult1 = narLoader.load(narFiles1);
        assertNotNull(narLoadResult1);
        assertEquals(0, narLoadResult1.getLoadedBundles().size());
        assertEquals(1, narLoadResult1.getSkippedBundles().size());

        // Copy the service impl which also depends on service API NAR
        final File serviceImplNar = new File(extensionsDir, "nifi-example-service-nar-1.1.nar");
        final File targetServiceImplNar = new File(narAutoLoadDir.toFile(), serviceImplNar.getName());
        Files.copy(serviceImplNar.toPath(), targetServiceImplNar.toPath(), StandardCopyOption.REPLACE_EXISTING);

        // Attempt to load while processor and service impl NARs available
        final List<File> narFiles2 = Arrays.asList(targetServiceImplNar);
        final NarLoadResult narLoadResult2 = narLoader.load(narFiles2);
        assertNotNull(narLoadResult2);
        assertEquals(0, narLoadResult2.getLoadedBundles().size());
        assertEquals(2, narLoadResult2.getSkippedBundles().size());

        // Copy service API NAR
        final File serviceApiNar = new File(extensionsDir, "nifi-example-service-api-nar-1.0.nar");
        final File targetServiceApiNar = new File(narAutoLoadDir.toFile(), serviceApiNar.getName());
        Files.copy(serviceApiNar.toPath(), targetServiceApiNar.toPath(), StandardCopyOption.REPLACE_EXISTING);

        // Attempt to load while all NARs available
        final List<File> narFiles3 = Arrays.asList(targetServiceApiNar);
        final NarLoadResult narLoadResult3 = narLoader.load(narFiles3);
        assertNotNull(narLoadResult3);
        assertEquals(3, narLoadResult3.getLoadedBundles().size());
        assertEquals(0, narLoadResult3.getSkippedBundles().size());

        assertEquals(5, narClassLoaders.getBundles().size());
        assertEquals(1, extensionManager.getExtensions(Processor.class).size());
        assertEquals(1, extensionManager.getExtensions(ControllerService.class).size());
        assertEquals(0, extensionManager.getExtensions(ReportingTask.class).size());
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
