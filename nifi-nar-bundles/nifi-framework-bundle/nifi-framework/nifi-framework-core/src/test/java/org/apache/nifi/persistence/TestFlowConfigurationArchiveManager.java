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
package org.apache.nifi.persistence;

import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.FormatUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFlowConfigurationArchiveManager {

    private static final Logger logger = LoggerFactory.getLogger(TestFlowConfigurationArchiveManager.class);
    private final File flowFile = new File("./target/flow-archive/flow.xml.gz");
    private final File archiveDir = new File("./target/flow-archive");
    private final long maxTime = FormatUtils.getTimeDuration("30 days", TimeUnit.MILLISECONDS);
    private long maxStorage = DataUnit.parseDataSize("500 MB", DataUnit.B).longValue();

    @Before
    public void before() throws Exception {

        // Clean up old files.
        if (Files.isDirectory(archiveDir.toPath())) {
            Files.walkFileTree(archiveDir.toPath(), new SimpleFileVisitor<Path>(){
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        // Create original flow.xml.gz
        Files.createDirectories(flowFile.getParentFile().toPath());
        try (OutputStream os = Files.newOutputStream(flowFile.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            // 10 bytes.
            os.write("0123456789".getBytes());
        }

    }

    @Test(expected = NoSuchFileException.class)
    public void testArchiveWithoutOriginalFile() throws Exception {
        final File flowFile = new File("does-not-exist");
        final FlowConfigurationArchiveManager archiveManager =
                new FlowConfigurationArchiveManager(flowFile.toPath(), archiveDir.toPath(), maxTime, maxStorage);

        archiveManager.archive();
    }

    private void createSimulatedOldArchives(final File[] oldArchives, final long intervalMillis) throws Exception {

        // Create old archive files. Altering file name and last modified date to simulate existing files.
        final long now = System.currentTimeMillis();
        final SimpleDateFormat dateFormat = new SimpleDateFormat("HHmmss");

        FlowConfigurationArchiveManager archiveManager =
                new FlowConfigurationArchiveManager(flowFile.toPath(), archiveDir.toPath(), maxTime, maxStorage);

        for (int i = oldArchives.length; i > 0; i--) {
            final Date date = new Date(now - (intervalMillis * i));
            final String hhmmss = dateFormat.format(date);

            final File archiveFile = archiveManager.archive();
            final String renamedArchiveName = archiveFile.getName().replaceFirst("T[\\d]{6}", "T" + hhmmss);
            final File renamedArchive = archiveFile.getParentFile().toPath().resolve(renamedArchiveName).toFile();
            archiveFile.renameTo(renamedArchive);

            Files.setLastModifiedTime(renamedArchive.toPath(), FileTime.fromMillis(date.getTime()));

            oldArchives[oldArchives.length - i] = renamedArchive;
        }
    }

    @Test
    public void testArchiveExpiration() throws Exception {

        final long intervalMillis = 60_000;
        File[] oldArchives = new File[5];
        createSimulatedOldArchives(oldArchives, intervalMillis);

        // Now, we will test expiration. There should be following old archives created above:
        // -5 min, -4 min, -3min, -2min, -1min
        // if maxTime = 3.5min, The oldest two files should be removed, -5 min and -4 min,
        // resulting four files of -3min, -2min, -1min, and newly created archive.
        final long maxTimeForExpirationTest = intervalMillis * 3 + (intervalMillis / 2);
        FlowConfigurationArchiveManager archiveManager = new FlowConfigurationArchiveManager(flowFile.toPath(),
                archiveDir.toPath(), maxTimeForExpirationTest, maxStorage);

        final File archive = archiveManager.archive();
        assertTrue(archive.isFile());

        assertFalse(oldArchives[0].exists());
        assertFalse(oldArchives[1].exists());
        assertTrue(oldArchives[2].isFile());
        assertTrue(oldArchives[3].isFile());
        assertTrue(oldArchives[4].isFile());

        assertTrue("Original file should remain intact", flowFile.isFile());
    }


    @Test
    public void testArchiveStorageSizeLimit() throws Exception {

        final long intervalMillis = 60_000;
        File[] oldArchives = new File[5];
        createSimulatedOldArchives(oldArchives, intervalMillis);

        // Now, we will test storage size limit. There should be following old archives created above:
        // -5 min, -4 min, -3min, -2min, -1min, each of those have 10 bytes.
        // if maxStorage = 20 bytes, The oldest four files should be removed,
        // resulting two files of -1min, and newly created archive, 20 bytes in total.
        FlowConfigurationArchiveManager archiveManager = new FlowConfigurationArchiveManager(flowFile.toPath(),
                archiveDir.toPath(), maxTime, 20);

        final File archive = archiveManager.archive();
        assertTrue(archive.isFile());

        assertFalse(oldArchives[0].exists());
        assertFalse(oldArchives[1].exists());
        assertFalse(oldArchives[2].exists());
        assertFalse(oldArchives[3].exists());
        assertTrue(oldArchives[4].isFile());

        assertTrue("Original file should remain intact", flowFile.isFile());
    }


}
