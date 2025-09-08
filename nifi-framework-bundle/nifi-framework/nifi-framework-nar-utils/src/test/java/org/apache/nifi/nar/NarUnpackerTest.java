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

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NarUnpackerTest {

    private static final String PROPERTIES_PATH = "/NarUnpacker/conf/nifi.properties";

    private static final String GROUP_ID = "org.apache.nifi";

    private static final String ARTIFACT_ID = "nifi-nar";

    private static final String VERSION = "1.0.0";

    @BeforeAll
    public static void copyResources() throws IOException {
        final Path sourcePath = Paths.get("./src/test/resources");
        final Path targetPath = Paths.get("./target");

        Files.walkFileTree(sourcePath, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

                Path relativeSource = sourcePath.relativize(dir);
                Path target = targetPath.resolve(relativeSource);

                Files.createDirectories(target);

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                Path relativeSource = sourcePath.relativize(file);
                Path target = targetPath.resolve(relativeSource);

                Files.copy(file, target, REPLACE_EXISTING);

                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Test
    public void testUnpackNars() {
        NiFiProperties properties = loadSpecifiedProperties(Collections.emptyMap());

        assertEquals("./target/NarUnpacker/lib/",
                properties.getProperty("nifi.nar.library.directory"));
        assertEquals("./target/NarUnpacker/lib2/",
                properties.getProperty("nifi.nar.library.directory.alt"));

        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, SystemBundle.create(properties), NarUnpackMode.UNPACK_INDIVIDUAL_JARS);

        assertEquals(2, extensionMapping.getAllExtensionNames().size());

        assertTrue(extensionMapping.getAllExtensionNames().containsKey("org.apache.nifi.processors.dummy.one"));
        assertTrue(extensionMapping.getAllExtensionNames().containsKey("org.apache.nifi.processors.dummy.two"));
        final File extensionsWorkingDir = properties.getExtensionsWorkingDirectory();
        File[] extensionFiles = Objects.requireNonNull(extensionsWorkingDir.listFiles());

        Set<String> expectedNars = new HashSet<>();
        expectedNars.add("dummy-one.nar-unpacked");
        expectedNars.add("dummy-two.nar-unpacked");
        expectedNars.add("nifi-jetty-nar.nar-unpacked");
        assertEquals(expectedNars.size(), extensionFiles.length);

        for (File extensionFile : extensionFiles) {
            assertTrue(expectedNars.contains(extensionFile.getName()));
        }
    }

    @Test
    public void testUnpackNarsFromEmptyDir() {
        final File emptyDir = new File("./target/empty/dir");
        emptyDir.deleteOnExit();
        assertTrue(emptyDir.mkdirs());

        final Map<String, String> others = new HashMap<>();
        others.put("nifi.nar.library.directory.alt", emptyDir.toString());
        NiFiProperties properties = loadSpecifiedProperties(others);

        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, SystemBundle.create(properties), NarUnpackMode.UNPACK_INDIVIDUAL_JARS);

        assertEquals(1, extensionMapping.getAllExtensionNames().size());
        assertTrue(extensionMapping.getAllExtensionNames().containsKey("org.apache.nifi.processors.dummy.one"));

        final File extensionsWorkingDir = properties.getExtensionsWorkingDirectory();
        File[] extensionFiles = Objects.requireNonNull(extensionsWorkingDir.listFiles());

        assertEquals(2, extensionFiles.length);

        final Optional<File> foundDummyOne = Stream.of(extensionFiles)
                .filter(f -> f.getName().equals("dummy-one.nar-unpacked"))
                .findFirst();
        assertTrue(foundDummyOne.isPresent());
    }

    @Test
    public void testUnpackNarsFromNonExistantDir() {
        final File nonExistantDir = new File("./target/this/dir/should/not/exist/");
        nonExistantDir.deleteOnExit();

        final Map<String, String> others = new HashMap<>();
        others.put("nifi.nar.library.directory.alt", nonExistantDir.toString());
        NiFiProperties properties = loadSpecifiedProperties(others);

        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, SystemBundle.create(properties), NarUnpackMode.UNPACK_INDIVIDUAL_JARS);

        assertTrue(extensionMapping.getAllExtensionNames().containsKey("org.apache.nifi.processors.dummy.one"));

        assertEquals(1, extensionMapping.getAllExtensionNames().size());

        final File extensionsWorkingDir = properties.getExtensionsWorkingDirectory();
        File[] extensionFiles = Objects.requireNonNull(extensionsWorkingDir.listFiles());

        assertEquals(2, extensionFiles.length);

        final Optional<File> foundDummyOne = Stream.of(extensionFiles)
                .filter(f -> f.getName().equals("dummy-one.nar-unpacked"))
                .findFirst();
        assertTrue(foundDummyOne.isPresent());
    }

    @Test
    public void testUnpackNarsFromNonDir() throws IOException {
        final File nonDir = new File("./target/file.txt");
        assertTrue(nonDir.createNewFile());
        nonDir.deleteOnExit();

        final Map<String, String> others = new HashMap<>();
        others.put("nifi.nar.library.directory.alt", nonDir.toString());
        NiFiProperties properties = loadSpecifiedProperties(others);

        final ExtensionMapping extensionMapping = NarUnpacker.unpackNars(properties, SystemBundle.create(properties), NarUnpackMode.UNPACK_INDIVIDUAL_JARS);

        assertNull(extensionMapping);
    }

    @Test
    public void testMapExtensionNoDependencies(@TempDir final Path tempDir) throws IOException {
        final File unpackedNarDir = tempDir.resolve(ARTIFACT_ID).toFile();

        final BundleCoordinate bundleCoordinate = new BundleCoordinate(GROUP_ID, ARTIFACT_ID, VERSION);
        final ExtensionMapping extensionMapping = new ExtensionMapping();

        NarUnpacker.mapExtension(unpackedNarDir, bundleCoordinate, extensionMapping);

        assertTrue(extensionMapping.isEmpty());
    }

    @Test
    public void testUnpackNarHandlesManifestBeforeMetaInfDirectory(@TempDir final Path tempDir) throws IOException {
        // Create a minimal NAR with problematic entry order: MANIFEST first, then META-INF/ directory
        final File narFile = tempDir.resolve("bad-order.nar").toFile();
        try (FileOutputStream fos = new FileOutputStream(narFile);
             JarOutputStream jos = new JarOutputStream(fos)) {
            // MANIFEST first (without prior META-INF/ dir entry)
            final JarEntry manifestEntry = new JarEntry("META-INF/MANIFEST.MF");
            jos.putNextEntry(manifestEntry);
            final byte[] manifestBytes = (
                    "Manifest-Version: 1.0\n" +
                    "NAR-Group: org.example\n" +
                    "NAR-Id: test-nar\n" +
                    "NAR-Version: 1.0.0\n\n"
            ).getBytes(StandardCharsets.UTF_8);
            jos.write(manifestBytes);
            jos.closeEntry();

            // META-INF/ directory entry added after
            final JarEntry metaInfDir = new JarEntry("META-INF/");
            jos.putNextEntry(metaInfDir);
            jos.closeEntry();
        }

        final File baseWorkingDir = tempDir.resolve("work").toFile();

        // Should unpack successfully despite directory entry order
        final File unpackedDir = NarUnpacker.unpackNar(narFile, baseWorkingDir, true, NarUnpackMode.UNPACK_INDIVIDUAL_JARS);
        final File manifestOut = new File(unpackedDir, "META-INF/MANIFEST.MF");
        assertTrue(manifestOut.isFile(), "Manifest should be unpacked even when META-INF/ entry comes later");
    }

    private NiFiProperties loadSpecifiedProperties(final Map<String, String> others) {
        String filePath;
        try {
            filePath = Objects.requireNonNull(NarUnpackerTest.class.getResource(PROPERTIES_PATH)).toURI().getPath();
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Cannot load properties file due to " + ex.getLocalizedMessage(), ex);
        }
        return NiFiProperties.createBasicNiFiProperties(filePath, others);
    }
}
