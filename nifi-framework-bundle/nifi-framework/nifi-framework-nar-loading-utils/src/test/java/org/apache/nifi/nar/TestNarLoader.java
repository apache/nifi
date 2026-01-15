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

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.ReportingTask;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class TestNarLoader extends AbstractTestNarLoader {
    static final String WORK_DIR = "work";
    static final String NAR_AUTOLOAD_DIR = "extensions";
    static final String PROPERTIES_FILE = "./src/test/resources/conf/nifi.properties";
    static final String EXTENSIONS_DIR = "./src/test/resources/extensions";

    @Test
    public void testNarLoaderWhenAllAvailable() throws IOException {
        // Copy all NARs from src/test/resources/extensions to <temporary directory>/extensions
        final Path extensionsDir = Paths.get(EXTENSIONS_DIR);
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(extensionsDir)) {
            for (Path sourcePath : directoryStream) {
                Files.copy(sourcePath, narAutoLoadDir.resolve(sourcePath.getFileName()));
            }
        }

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(narAutoLoadDir)) {
            final List<File> narFiles = new ArrayList<>();
            for (Path entry : directoryStream) {
                narFiles.add(entry.toFile());
            }

            assertEquals(3, narFiles.size());

            final NarLoadResult narLoadResult = narLoader.load(narFiles);
            assertNotNull(narLoadResult);
            assertEquals(3, narLoadResult.getLoadedBundles().size());
            assertEquals(0, narLoadResult.getSkippedBundles().size());

            assertEquals(6, narClassLoaders.getBundles().size());
            assertEquals(1, extensionManager.getExtensions(Processor.class).size());
            assertEquals(1, extensionManager.getExtensions(ControllerService.class).size());
            assertEquals(0, extensionManager.getExtensions(ReportingTask.class).size());
        }
    }

    @Test
    public void testNarLoaderWhenDependentNarsAreMissing() throws IOException {
        final Path extensionsDir = Paths.get(EXTENSIONS_DIR);
        final Path processorsNar = extensionsDir.resolve("nifi-example-processors-nar-1.0.nar");
        // Copy processors NAR first which depends on service API NAR
        final Path targetProcessorNar = narAutoLoadDir.resolve(processorsNar.getFileName());
        Files.copy(processorsNar, targetProcessorNar);

        // Attempt to load while only processor NAR is available
        final List<File> narFiles1 = List.of(targetProcessorNar.toFile());
        final NarLoadResult narLoadResult1 = narLoader.load(narFiles1);
        assertNotNull(narLoadResult1);
        assertEquals(0, narLoadResult1.getLoadedBundles().size());
        assertEquals(1, narLoadResult1.getSkippedBundles().size());

        // Copy the service impl which also depends on service API NAR
        final Path serviceImplNar = extensionsDir.resolve("nifi-example-service-nar-1.1.nar");
        final Path targetServiceImplNar = narAutoLoadDir.resolve(serviceImplNar.getFileName());
        Files.copy(serviceImplNar, targetServiceImplNar);

        // Attempt to load while processor and service impl NARs available
        final List<File> narFiles2 = List.of(targetServiceImplNar.toFile());
        final NarLoadResult narLoadResult2 = narLoader.load(narFiles2);
        assertNotNull(narLoadResult2);
        assertEquals(0, narLoadResult2.getLoadedBundles().size());
        assertEquals(2, narLoadResult2.getSkippedBundles().size());

        // Copy service API NAR
        final Path serviceApiNar = extensionsDir.resolve("nifi-example-service-api-nar-1.0.nar");
        final Path targetServiceApiNar = narAutoLoadDir.resolve(serviceApiNar.getFileName());
        Files.copy(serviceApiNar, targetServiceApiNar);

        // Attempt to load while all NARs available
        final List<File> narFiles3 = List.of(targetServiceApiNar.toFile());
        final NarLoadResult narLoadResult3 = narLoader.load(narFiles3);
        assertNotNull(narLoadResult3);
        assertEquals(3, narLoadResult3.getLoadedBundles().size());
        assertEquals(0, narLoadResult3.getSkippedBundles().size());

        assertEquals(6, narClassLoaders.getBundles().size());
        assertEquals(1, extensionManager.getExtensions(Processor.class).size());
        assertEquals(1, extensionManager.getExtensions(ControllerService.class).size());
        assertEquals(0, extensionManager.getExtensions(ReportingTask.class).size());
    }

    @Override
    String getWorkDir() {
        return WORK_DIR;
    }

    @Override
    String getNarAutoloadDir() {
        return NAR_AUTOLOAD_DIR;
    }

    @Override
    String getPropertiesFile() {
        return PROPERTIES_FILE;
    }
}
