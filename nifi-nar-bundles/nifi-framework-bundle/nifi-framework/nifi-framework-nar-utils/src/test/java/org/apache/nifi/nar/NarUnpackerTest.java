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

import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NarUnpackerTest {

    @BeforeClass
    public static void copyResources() throws IOException {

        final Path sourcePath = Paths.get("./src/test/resources");
        final Path targetPath = Paths.get("./target");

        Files.walkFileTree(sourcePath, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {

                Path relativeSource = sourcePath.relativize(dir);
                Path target = targetPath.resolve(relativeSource);

                Files.createDirectories(target);

                return FileVisitResult.CONTINUE;

            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {

                Path relativeSource = sourcePath.relativize(file);
                Path target = targetPath.resolve(relativeSource);

                Files.copy(file, target, REPLACE_EXISTING);

                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Test
    public void testUnpackNars() {

        NiFiProperties properties = loadSpecifiedProperties("/NarUnpacker/conf/nifi.properties", Collections.EMPTY_MAP);

        assertEquals("./target/NarUnpacker/lib/",
                properties.getProperty("nifi.nar.library.directory"));
        assertEquals("./target/NarUnpacker/lib2/",
                properties.getProperty("nifi.nar.library.directory.alt"));

        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, SystemBundle.create(properties));

        assertEquals(2, extensionMapping.getAllExtensionNames().size());

        assertTrue(extensionMapping.getAllExtensionNames().keySet().contains("org.apache.nifi.processors.dummy.one"));
        assertTrue(extensionMapping.getAllExtensionNames().keySet().contains("org.apache.nifi.processors.dummy.two"));
        final File extensionsWorkingDir = properties.getExtensionsWorkingDirectory();
        File[] extensionFiles = extensionsWorkingDir.listFiles();

        Set<String> expectedNars = new HashSet<>();
        expectedNars.add("dummy-one.nar-unpacked");
        expectedNars.add("dummy-two.nar-unpacked");
        assertEquals(expectedNars.size(), extensionFiles.length);

        for (File extensionFile : extensionFiles) {
            Assert.assertTrue(expectedNars.contains(extensionFile.getName()));
        }
    }

    @Test
    public void testUnpackNarsFromEmptyDir() throws IOException {

        final File emptyDir = new File("./target/empty/dir");
        emptyDir.delete();
        emptyDir.deleteOnExit();
        assertTrue(emptyDir.mkdirs());

        final Map<String, String> others = new HashMap<>();
        others.put("nifi.nar.library.directory.alt", emptyDir.toString());
        NiFiProperties properties = loadSpecifiedProperties("/NarUnpacker/conf/nifi.properties", others);

        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, SystemBundle.create(properties));

        assertEquals(1, extensionMapping.getAllExtensionNames().size());
        assertTrue(extensionMapping.getAllExtensionNames().keySet().contains("org.apache.nifi.processors.dummy.one"));

        final File extensionsWorkingDir = properties.getExtensionsWorkingDirectory();
        File[] extensionFiles = extensionsWorkingDir.listFiles();

        assertEquals(1, extensionFiles.length);
        assertEquals("dummy-one.nar-unpacked", extensionFiles[0].getName());
    }

    @Test
    public void testUnpackNarsFromNonExistantDir() {

        final File nonExistantDir = new File("./target/this/dir/should/not/exist/");
        nonExistantDir.delete();
        nonExistantDir.deleteOnExit();

        final Map<String, String> others = new HashMap<>();
        others.put("nifi.nar.library.directory.alt", nonExistantDir.toString());
        NiFiProperties properties = loadSpecifiedProperties("/NarUnpacker/conf/nifi.properties", others);

        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, SystemBundle.create(properties));

        assertTrue(extensionMapping.getAllExtensionNames().keySet().contains("org.apache.nifi.processors.dummy.one"));

        assertEquals(1, extensionMapping.getAllExtensionNames().size());

        final File extensionsWorkingDir = properties.getExtensionsWorkingDirectory();
        File[] extensionFiles = extensionsWorkingDir.listFiles();

        assertEquals(1, extensionFiles.length);
        assertEquals("dummy-one.nar-unpacked", extensionFiles[0].getName());
    }

    @Test
    public void testUnpackNarsFromNonDir() throws IOException {

        final File nonDir = new File("./target/file.txt");
        nonDir.createNewFile();
        nonDir.deleteOnExit();

        final Map<String, String> others = new HashMap<>();
        others.put("nifi.nar.library.directory.alt", nonDir.toString());
        NiFiProperties properties = loadSpecifiedProperties("/NarUnpacker/conf/nifi.properties", others);

        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, SystemBundle.create(properties));

        assertNull(extensionMapping);
    }

    private NiFiProperties loadSpecifiedProperties(final String propertiesFile, final Map<String, String> others) {
        String filePath;
        try {
            filePath = NarUnpackerTest.class.getResource(propertiesFile).toURI().getPath();
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Cannot load properties file due to " + ex.getLocalizedMessage(), ex);
        }
        return NiFiProperties.createBasicNiFiProperties(filePath, others);
    }
}
