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

package org.apache.nifi.tests.system;

import org.junit.jupiter.api.AfterEach;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for system tests that exercise a component's migration path by swapping the
 * {@code nifi-system-test-extensions} NARs for the {@code nifi-alternate-config-extensions} NAR between the
 * pre-migration setup phase and the assertion phase. Subclasses call {@link #switchOutNars()} once NiFi has been
 * stopped for the migration boundary, and this class restores the original layout via {@link #switchNarsBack()} in
 * an {@link AfterEach} hook so subsequent tests see the default NAR set.
 */
public abstract class AbstractNarSwapMigrationIT extends NiFiSystemIT {

    private boolean narsSwapped;

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @AfterEach
    public void restoreNars() {
        if (!narsSwapped && getNiFiInstance().isAccessible()) {
            return;
        }

        // Stop the NiFi instance, ensure that the nifi-system-test-extensions-nar and nifi-alternate-config-extensions bundles
        // are where they need to be. Then, restart the instance so that everything is in the right state for the next test that
        // will run
        getNiFiInstance().stop();
        if (narsSwapped) {
            switchNarsBack();
            narsSwapped = false;
        }
        getNiFiInstance().start(true);
    }

    protected void switchOutNars() throws IOException {
        narsSwapped = true;
        for (final File instanceDir : instanceDirectories()) {
            switchOutNars(instanceDir);
        }
    }

    private void switchOutNars(final File instanceDir) throws IOException {
        final File lib = new File(instanceDir, "lib");
        final File alternateConfig = new File(lib, "alternate-config");

        // Move the nifi-system-test-extensions-nar out of the lib directory
        moveNars(lib, "nifi-system-test-extensions-nar-.*", alternateConfig);

        // Move the nifi-system-test-extensions-services-nar out of the lib directory
        moveNars(lib, "nifi-system-test-extensions-services-nar-.*", alternateConfig);

        // Move the nifi-system-test-extensions-services-api-nar out of the lib directory
        moveNars(lib, "nifi-system-test-extensions-services-api-nar-.*", alternateConfig);

        moveNars(alternateConfig, "nifi-alternate-config.*", lib);

        final File workDir = new File(instanceDir, "work/nar/extensions");
        deleteRecursively(workDir);
    }

    protected void switchNarsBack() {
        for (final File instanceDir : instanceDirectories()) {
            switchNarsBack(instanceDir);
        }
    }

    private void switchNarsBack(final File instanceDir) {
        final File lib = new File(instanceDir, "lib");
        final File alternateConfig = new File(lib, "alternate-config");
        if (findFile(lib, "nifi-system-test-extensions-nar-.*") != null) {
            return;
        }

        // Move the nifi-system-test-extensions-nar back to the lib directory
        moveNars(alternateConfig, "nifi-system-test-extensions-nar-.*", lib);

        // Move the nifi-system-test-extensions-services-nar back to the lib directory
        moveNars(alternateConfig, "nifi-system-test-extensions-services-nar-.*", lib);

        // Move the nifi-system-test-extensions-services-api-nar back to the lib directory
        moveNars(alternateConfig, "nifi-system-test-extensions-services-api-nar-.*", lib);

        moveNars(lib, "nifi-alternate-config.*", alternateConfig);
    }

    /**
     * Standalone instances report a single directory. A cluster's aggregate instance has no directory of its own,
     * so the NAR swap has to visit each node.
     */
    private List<File> instanceDirectories() {
        final NiFiInstance instance = getNiFiInstance();
        if (instance.getNumberOfNodes() <= 1) {
            return List.of(instance.getInstanceDirectory());
        }

        final List<File> directories = new ArrayList<>();
        for (int nodeIndex = 1; nodeIndex <= instance.getNumberOfNodes(); nodeIndex++) {
            directories.add(instance.getNodeInstance(nodeIndex).getInstanceDirectory());
        }
        return directories;
    }

    private void deleteRecursively(final File file) throws IOException {
        Files.walkFileTree(file.toPath(), new FileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path visited, final BasicFileAttributes attrs) throws IOException {
                Files.delete(visited);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(final Path visited, final IOException exc) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private File findFile(final File dir, final String regex) {
        final Pattern pattern = Pattern.compile(regex);
        final File[] files = dir.listFiles(file -> pattern.matcher(file.getName()).find());
        if (files == null || files.length != 1) {
            return null;
        }
        return files[0];
    }

    private void moveNars(final File source, final String regex, final File target) {
        final File libNar = findFile(source, regex);
        assertNotNull(libNar);
        final File libNarTarget = new File(target, libNar.getName());
        assertTrue(libNar.renameTo(libNarTarget));
    }
}
