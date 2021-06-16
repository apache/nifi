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

package org.apache.nifi.components.resource;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestStandardResourceReferences {

    @Test
    public void testAsURLs() {
        final ResourceReferences references = new StandardResourceReferences(Arrays.asList(
            new FileResourceReference(new File("target/1.txt")),
            new FileResourceReference(new File("target/2.txt"))
        ));

        final List<URL> urls = references.asURLs();
        assertEquals(2, urls.size());
        for (final URL url : urls) {
            final String filename = url.getFile();
            assertTrue(filename.endsWith(".txt"));

            final File parentFile = new File(filename).getParentFile();
            assertEquals("target", parentFile.getName());
        }
    }

    @Test
    public void testFlattenRecursively() throws IOException {
        // Create directory structure:
        // target/dirs/
        // target/dirs/0
        // target/dirs/0/0
        // target/dirs/0/0/0.txt
        // target/dirs/0/0/1.txt
        // ...
        // target/dirs/2/2/2.txt
        final int numDirs = 3;
        final int numSubDirs = 3;
        final int numFiles = 3;

        final File baseDir = new File("target/dirs");
        for (int i=0; i < numDirs; i++) {
            final File dir = new File(baseDir, String.valueOf(i));
            dir.mkdirs();

            for (int j=0; j < numSubDirs; j++) {
                final File subdir = new File(dir, String.valueOf(j));
                subdir.mkdirs();

                for (int k=0; k < numFiles; k++) {
                    final File file = new File(subdir, k + ".txt");

                    try (final OutputStream fos = new FileOutputStream(file)) {
                        fos.write((k + ".txt").getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
        }

        final ResourceReferences references = new StandardResourceReferences(Collections.singletonList(new FileResourceReference(baseDir)));
        assertEquals(1, references.getCount());
        assertEquals(ResourceType.DIRECTORY, references.asList().get(0).getResourceType());

        final ResourceReferences flattened = references.flattenRecursively();
        assertEquals(numDirs * numSubDirs * numFiles, flattened.getCount());

        final List<ResourceReference> flattenedReferences = flattened.asList();
        assertEquals(numDirs * numSubDirs * numFiles, flattenedReferences.size());

        // Ensure that each file that was flattened has a unique filename and the file exists.
        final Set<String> filenames = new HashSet<>();
        for (final ResourceReference reference : flattenedReferences) {
            assertEquals(ResourceType.FILE, reference.getResourceType());

            final String filename = reference.getLocation();
            assertTrue(filename.endsWith(".txt"));

            filenames.add(filename);
            assertTrue(new File(filename).exists());
        }

        assertEquals(numDirs * numSubDirs * numFiles, filenames.size());
    }

    @Test
    public void testFlatten() throws IOException {
        // Create directory structure:
        // target/dir
        // target/dir/0
        // target/dir/0/0.txt
        // target/dir/0/1.txt
        // ...
        // target/dir/0.txt
        // target/dir/1.txt
        // ...
        final int numFiles = 3;

        final File baseDir = new File("target/dir");
        baseDir.mkdirs();

        for (int i=0; i < numFiles; i++) {
            final File file = new File(baseDir, i + ".txt");

            try (final OutputStream fos = new FileOutputStream(file)) {
                fos.write((i + ".txt").getBytes(StandardCharsets.UTF_8));
            }
        }

        final ResourceReferences references = new StandardResourceReferences(Collections.singletonList(new FileResourceReference(baseDir)));
        assertEquals(1, references.getCount());
        assertEquals(ResourceType.DIRECTORY, references.asList().get(0).getResourceType());

        final ResourceReferences flattened = references.flatten();
        assertEquals(numFiles, flattened.getCount());

        final List<ResourceReference> flattenedReferences = flattened.asList();
        assertEquals(numFiles, flattenedReferences.size());

        // Ensure that each file that was flattened has a unique filename and the file exists.
        final Set<String> filenames = new HashSet<>();
        for (final ResourceReference reference : flattenedReferences) {
            assertEquals(ResourceType.FILE, reference.getResourceType());

            final String filename = reference.getLocation();
            assertTrue(filename.endsWith(".txt"));

            filenames.add(filename);
            assertTrue(new File(filename).exists());
        }

        assertEquals(numFiles, filenames.size());
    }
}
