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

package org.apache.nifi.stateless.repository;

import org.apache.nifi.controller.repository.ContentRepositoryContext;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.stream.io.StreamUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestStatelessFileSystemContentRepository {
    private final File repoDirectory = new File("target/test-stateless-file-system-repository");
    private StatelessFileSystemContentRepository repository;

    private final ContentRepositoryContext contentRepositoryContext = new ContentRepositoryContext() {
        @Override
        public ResourceClaimManager getResourceClaimManager() {
            return new StandardResourceClaimManager();
        }

        @Override
        public EventReporter getEventReporter() {
            return EventReporter.NO_OP;
        }
    };

    @BeforeEach
    public void setup() throws IOException {
        repository = new StatelessFileSystemContentRepository(repoDirectory);
        repository.initialize(contentRepositoryContext);
    }

    @AfterEach
    public void cleanup() {
        repository.cleanup();
    }

    @Test
    public void testWriteThenRead() throws IOException {
        final byte[] contents = "Hello, World!".getBytes();
        final ContentClaim claim = repository.create(true);
        try (final OutputStream out = repository.write(claim)) {
            out.write(contents);
        }

        assertEquals(contents.length, claim.getLength());

        // Ensure we can read multiple times.
        for (int i=0; i < 5; i++) {
            final byte[] bytesRead;
            try (final InputStream in = repository.read(claim);
                 final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                StreamUtils.copy(in, baos);
                bytesRead = baos.toByteArray();
            }

            assertEquals(new String(contents), new String(bytesRead));
        }

        File[] files = repoDirectory.listFiles();
        assertNotNull(files);
        assertEquals(1, files.length);

        repository.purge();
        files = repoDirectory.listFiles();
        assertNotNull(files);
        assertEquals(0, files.length);
    }

    @Test
    public void testOverwriteFails() throws IOException {
        final byte[] contents = "Hello, World!".getBytes();
        final ContentClaim claim = repository.create(true);
        try (final OutputStream out = repository.write(claim)) {
            out.write(contents);
        }

        // An attempt to write to a content claim multiple times should fail
        assertThrows(IOException.class, () -> repository.write(claim));
    }

    @Test
    public void testOverwriteFailsBeforeClosingOutputStream() throws IOException {
        final byte[] contents = "Hello, World!".getBytes();
        final ContentClaim claim = repository.create(true);
        try (final OutputStream out = repository.write(claim)) {
            out.write(contents);

            // An attempt to write to a content claim multiple times should fail
            assertThrows(IOException.class, () -> repository.write(claim));
        }
    }

    @Test
    public void testWriteToMultipleStreams() throws IOException {
        final ContentClaim claim1 = repository.create(true);
        final ContentClaim claim2 = repository.create(true);
        final ContentClaim claim3 = repository.create(true);

        final OutputStream out1 = repository.write(claim1);
        final OutputStream out2 = repository.write(claim2);
        final OutputStream out3 = repository.write(claim3);

        for (final char c : "Hello World".toCharArray()) {
            out1.write(c);
            out2.write(c);
            out3.write(c);
        }

        out1.close();
        out2.close();
        out3.close();

        for (final ContentClaim claim : Arrays.asList(claim1, claim2, claim3)) {
            try (final InputStream in = repository.read(claim)) {
                for (final char c : "Hello World".toCharArray()) {
                    assertEquals(c, in.read());
                }

                assertEquals(-1, in.read());
            }
        }

        final ContentClaim claim4 = repository.create(true);
        final ResourceClaim resourceClaim4 = claim4.getResourceClaim();
        assertTrue(resourceClaim4.equals(claim1.getResourceClaim()) || resourceClaim4.equals(claim2.getResourceClaim()) || resourceClaim4.equals(claim3.getResourceClaim()));

        try (final OutputStream out4 = repository.write(claim4)) {
            out4.write("Hello World".getBytes());
        }

        try (final InputStream in = repository.read(claim4)) {
            for (final char c : "Hello World".toCharArray()) {
                assertEquals(c, in.read());
            }

            assertEquals(-1, in.read());
        }
    }
}
