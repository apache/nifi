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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.nifi.bundle.Bundle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@EnabledOnOs({ OS.MAC })
@DisabledIfSystemProperty(named = "os.arch", matches = "aarch64|arm64")
public class TestLoadNativeLibViaSystemProperty extends AbstractTestNarLoader {
    static final String WORK_DIR = "./target/work";
    static final String NAR_AUTOLOAD_DIR = "./target/nars_without_native_lib";
    static final String PROPERTIES_FILE = "./src/test/resources/conf/nifi.nar_without_native_lib.properties";
    static final String EXTENSIONS_DIR = "./src/test/resources/nars_without_native_lib";

    private static String oldJavaLibraryPath;

    @BeforeAll
    public static void setUpClass() {
        oldJavaLibraryPath = System.getProperty("java.library.path");
        System.setProperty("java.library.path", "./src/test/resources/native");
    }

    @AfterAll
    public static void tearDownSuite() {
        if (oldJavaLibraryPath != null) {
            System.setProperty("java.library.path", oldJavaLibraryPath);
            oldJavaLibraryPath = null;
        }
    }

    @Test
    public void testLoadSameLibraryByNarClassLoaderFromSystemProperty() throws Exception {
        final File extensionsDir = new File(EXTENSIONS_DIR);
        final Path narAutoLoadDir = Paths.get(NAR_AUTOLOAD_DIR);
        for (final File extensionFile : extensionsDir.listFiles()) {
            Files.copy(extensionFile.toPath(), narAutoLoadDir.resolve(extensionFile.getName()), StandardCopyOption.REPLACE_EXISTING);
        }

        final List<File> narFiles = Arrays.asList(narAutoLoadDir.toFile().listFiles());
        assertEquals(1, narFiles.size());

        final NarLoadResult narLoadResult = narLoader.load(narFiles);
        assertNotNull(narLoadResult);

        List<NarClassLoader> narClassLoaders = this.narClassLoaders.getBundles().stream()
                .filter(bundle -> bundle.getBundleDetails().getCoordinate().getCoordinate().contains("nifi-nar_without_native_lib-"))
                .map(Bundle::getClassLoader)
                .filter(NarClassLoader.class::isInstance)
                .map(NarClassLoader.class::cast)
                .toList();


        Set<String> actualLibraryLocations = narClassLoaders.stream()
                .map(classLoader -> classLoader.findLibrary("testjni"))
                .collect(Collectors.toSet());

        for (NarClassLoader narClassLoader : narClassLoaders) {
            Class<?> TestJNI = narClassLoader.loadClass("org.apache.nifi.nar.sharedlib.TestJNI");

            Object actualJniMethodReturnValue = TestJNI
                    .getMethod("testJniMethod")
                .invoke(TestJNI.getDeclaredConstructor().newInstance());

            assertEquals("calledNativeTestJniMethod", actualJniMethodReturnValue);
        }

        assertEquals(1, actualLibraryLocations.size());
        assertTrue(actualLibraryLocations.stream().anyMatch(location -> location.contains("nifi-nar_without_native_lib-1")));
    }

    @Test
    public void testLoadSameLibraryBy2InstanceClassLoadersFromSystemProperty() throws Exception {
        final File extensionsDir = new File(EXTENSIONS_DIR);
        final Path narAutoLoadDir = Paths.get(NAR_AUTOLOAD_DIR);
        for (final File extensionFile : extensionsDir.listFiles()) {
            Files.copy(extensionFile.toPath(), narAutoLoadDir.resolve(extensionFile.getName()), StandardCopyOption.REPLACE_EXISTING);
        }

        final List<File> narFiles = Arrays.asList(narAutoLoadDir.toFile().listFiles());
        assertEquals(1, narFiles.size());

        final NarLoadResult narLoadResult = narLoader.load(narFiles);
        assertNotNull(narLoadResult);

        Bundle bundleWithNativeLib = this.narClassLoaders.getBundles().stream()
                .filter(bundle -> bundle.getBundleDetails().getCoordinate().getCoordinate().contains("nifi-nar_without_native_lib-"))
                .findFirst().get();

        Class<?> processorClass = bundleWithNativeLib.getClassLoader().loadClass("org.apache.nifi.nar.ModifiesClasspathProcessor");

        List<InstanceClassLoader> instanceClassLoaders = Arrays.asList(
                extensionManager.createInstanceClassLoader(processorClass.getName(), UUID.randomUUID().toString(), bundleWithNativeLib, null),
                extensionManager.createInstanceClassLoader(processorClass.getName(), UUID.randomUUID().toString(), bundleWithNativeLib, null)
        );

        for (InstanceClassLoader instanceClassLoader : instanceClassLoaders) {
            String actualLibraryLocation = instanceClassLoader.findLibrary("testjni");

            Class<?> TestJNI = instanceClassLoader.loadClass("org.apache.nifi.nar.sharedlib.TestJNI");


            Object actualJniMethodReturnValue = TestJNI
                    .getMethod("testJniMethod")
                .invoke(TestJNI.getDeclaredConstructor().newInstance());

            assertTrue(actualLibraryLocation.contains(instanceClassLoader.getIdentifier()));
            assertEquals("calledNativeTestJniMethod", actualJniMethodReturnValue);
        }
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
