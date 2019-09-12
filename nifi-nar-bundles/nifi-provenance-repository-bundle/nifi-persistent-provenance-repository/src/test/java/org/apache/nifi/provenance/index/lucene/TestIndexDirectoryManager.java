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

package org.apache.nifi.provenance.index.lucene;

import org.apache.nifi.provenance.RepositoryConfiguration;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestIndexDirectoryManager {

    @Test
    public void testGetDirectoriesIncludesMatchingTimestampPlusOne() {
        final List<IndexLocation> locations = new ArrayList<>();
        locations.add(createLocation(999L));
        locations.add(createLocation(1002L));
        locations.add(createLocation(1005L));

        final List<File> directories = IndexDirectoryManager.getDirectories(1000L, 1001L, locations);
        assertEquals(2, directories.size());
        assertTrue(directories.contains(new File("lucene-8-index-999")));
        assertTrue(directories.contains(new File("lucene-8-index-1002")));
    }

    @Test
    public void testGetDirectoriesOnlyObtainsDirectoriesForDesiredPartition() {
        final RepositoryConfiguration config = createConfig(2);

        final File storageDir1 = config.getStorageDirectories().get("1");
        final File storageDir2 = config.getStorageDirectories().get("2");

        final File index1 = new File(storageDir1, "lucene-8-index-1");
        final File index2 = new File(storageDir1, "lucene-8-index-2");
        final File index3 = new File(storageDir2, "lucene-8-index-3");
        final File index4 = new File(storageDir2, "lucene-8-index-4");

        final File[] allIndices = new File[] {index1, index2, index3, index4};
        for (final File file : allIndices) {
            assertTrue(file.mkdirs() || file.exists());
        }

        try {
            final IndexDirectoryManager mgr = new IndexDirectoryManager(config);
            mgr.initialize();

            final List<File> indexes1 = mgr.getDirectories(0L, Long.MAX_VALUE, "1");
            final List<File> indexes2 = mgr.getDirectories(0L, Long.MAX_VALUE, "2");

            assertEquals(2, indexes1.size());
            assertTrue(indexes1.contains(index1));
            assertTrue(indexes1.contains(index2));

            assertEquals(2, indexes2.size());
            assertTrue(indexes2.contains(index3));
            assertTrue(indexes2.contains(index4));
        } finally {
            for (final File file : allIndices) {
                file.delete();
            }
        }
    }


    @Test
    public void testActiveIndexNotLostWhenSizeExceeded() throws IOException {
        final RepositoryConfiguration config = createConfig(2);
        config.setDesiredIndexSize(4096 * 128);

        final File storageDir1 = config.getStorageDirectories().get("1");
        final File storageDir2 = config.getStorageDirectories().get("2");

        final File index1 = new File(storageDir1, "lucene-8-index-1");
        final File index2 = new File(storageDir1, "lucene-8-index-2");
        final File index3 = new File(storageDir2, "lucene-8-index-3");
        final File index4 = new File(storageDir2, "lucene-8-index-4");

        final File[] allIndices = new File[] {index1, index2, index3, index4};
        for (final File file : allIndices) {
            assertTrue(file.mkdirs() || file.exists());
        }

        try {
            final IndexDirectoryManager mgr = new IndexDirectoryManager(config);
            mgr.initialize();

            File indexDir = mgr.getWritableIndexingDirectory(System.currentTimeMillis(), "1");
            assertTrue(indexDir.mkdirs() || indexDir.exists());

            final File newFile = new File(indexDir, "1.bin");
            try (final OutputStream fos = new FileOutputStream(newFile)) {
                final byte[] data = new byte[4096];
                for (int i = 0; i < 1024; i++) {
                    fos.write(data);
                }
            }

            try {
                final File newDir = mgr.getWritableIndexingDirectory(System.currentTimeMillis(), "1");
                assertEquals(indexDir, newDir);
            } finally {
                newFile.delete();
            }
        } finally {
            for (final File file : allIndices) {
                file.delete();
            }
        }
    }

    @Test
    public void testGetDirectoriesBefore() throws InterruptedException {
        final RepositoryConfiguration config = createConfig(2);
        config.setDesiredIndexSize(4096 * 128);

        final File storageDir = config.getStorageDirectories().get("1");

        final File index1 = new File(storageDir, "lucene-8-index-1");
        final File index2 = new File(storageDir, "lucene-8-index-2");

        final File[] allIndices = new File[] {index1, index2};
        for (final File file : allIndices) {
            if (file.exists()) {
                assertTrue(file.delete());
            }
        }

        assertTrue(index1.mkdirs());
        assertTrue(index2.mkdirs());
        final long timestamp = System.currentTimeMillis();

        try {
            final IndexDirectoryManager mgr = new IndexDirectoryManager(config);
            mgr.initialize();

            final List<File> dirsBefore = mgr.getDirectoriesBefore(timestamp);
            assertEquals(2, dirsBefore.size());
        } finally {
            for (final File file : allIndices) {
                file.delete();
            }
        }
    }


    private IndexLocation createLocation(final long timestamp) {
        return createLocation(timestamp, "1");
    }

    private IndexLocation createLocation(final long timestamp, final String partitionName) {
        return new IndexLocation(new File("lucene-8-index-" + timestamp), timestamp, partitionName);
    }

    private RepositoryConfiguration createConfig(final int partitions) {
        final RepositoryConfiguration repoConfig = new RepositoryConfiguration();
        for (int i = 1; i <= partitions; i++) {
            repoConfig.addStorageDirectory(String.valueOf(i), new File("target/storage/testIndexDirectoryManager/" + UUID.randomUUID() + "/" + i));
        }
        return repoConfig;
    }
}
