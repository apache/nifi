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

package org.apache.nifi.controller.repository.claim;

import org.apache.nifi.controller.repository.ContentClaimCreationContext;
import org.apache.nifi.controller.repository.FileSystemRepository;
import org.apache.nifi.controller.repository.LossTolerance;
import org.apache.nifi.controller.repository.StandardContentClaimCreationContext;
import org.apache.nifi.controller.repository.StandardContentRepositoryContext;
import org.apache.nifi.controller.repository.metrics.NopPerformanceTracker;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestStandardContentClaimWriteCache {
    private static final ContentClaimCreationContext CREATION_CONTEXT = new StandardContentClaimCreationContext("component-1", "connector-1", LossTolerance.LOSS_INTOLERANT);

    private static final int BUFFER_SIZE = 4;

    private static final Path NIFI_PROPERTIES_FILE = Paths.get("src/test/resources/conf/nifi.properties");

    @TempDir
    private Path tempDir;

    private final List<FileSystemRepository> repositories = new ArrayList<>();

    private FileSystemRepository repository = null;
    private StandardResourceClaimManager claimManager = null;

    @BeforeEach
    public void setup() throws IOException {
        claimManager = new StandardResourceClaimManager();
        repository = createRepository(Map.of());
    }

    @AfterEach
    public void shutdown() throws IOException {
        for (final FileSystemRepository createdRepository : repositories) {
            createdRepository.shutdown();
        }
    }

    private FileSystemRepository createRepository(final Map<String, String> additionalProperties) throws IOException {
        final Path repositoryDirectory = tempDir.resolve("content-repository-" + repositories.size());

        final Map<String, String> properties = new HashMap<>(additionalProperties);
        properties.put(NiFiProperties.REPOSITORY_CONTENT_PREFIX.concat("default"), repositoryDirectory.toString());

        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(NIFI_PROPERTIES_FILE.toString(), properties);
        final FileSystemRepository createdRepository = new FileSystemRepository(nifiProperties);
        createdRepository.initialize(new StandardContentRepositoryContext(claimManager, EventReporter.NO_OP));
        createdRepository.purge();

        repositories.add(createdRepository);
        return createdRepository;
    }

    @Test
    public void testFlushWriteCorrectData() throws IOException {
        final ContentClaimWriteCache cache = new StandardContentClaimWriteCache(repository, new NopPerformanceTracker(), CREATION_CONTEXT, BUFFER_SIZE);

        final ContentClaim claim1 = cache.getContentClaim();
        assertNotNull(claim1);

        final OutputStream out = cache.write(claim1);
        assertNotNull(out);
        out.write("hello".getBytes());
        out.write("good-bye".getBytes());

        cache.flush();

        assertEquals(13L, claim1.getLength());
        final InputStream in = repository.read(claim1);
        final byte[] buff = new byte[(int) claim1.getLength()];
        StreamUtils.fillBuffer(in, buff);
        assertArrayEquals("hellogood-bye".getBytes(), buff);

        final ContentClaim claim2 = cache.getContentClaim();
        final OutputStream out2 = cache.write(claim2);
        assertNotNull(out2);
        out2.write("good-day".getBytes());
        out2.write("hello".getBytes());

        cache.flush();

        assertEquals(13L, claim2.getLength());
        final InputStream in2 = repository.read(claim2);
        final byte[] buff2 = new byte[(int) claim2.getLength()];
        StreamUtils.fillBuffer(in2, buff2);
        assertArrayEquals("good-dayhello".getBytes(), buff2);
    }

    @Test
    public void testWriteLargeRollsOverToNewFileOnNext() throws IOException {
        final ContentClaimWriteCache cache = new StandardContentClaimWriteCache(repository, new NopPerformanceTracker(), CREATION_CONTEXT, BUFFER_SIZE);

        final ContentClaim claim1 = cache.getContentClaim();
        assertNotNull(claim1);

        try (final OutputStream out = cache.write(claim1)) {
            assertNotNull(out);
            out.write("hello".getBytes());
            out.write("good-bye".getBytes());

            cache.flush();
        }

        final ContentClaim claim2 = cache.getContentClaim();
        assertEquals(claim1.getResourceClaim(), claim2.getResourceClaim());

        try (final OutputStream out = cache.write(claim2)) {
            assertNotNull(out);
            out.write("greeting".getBytes());
        }

        final ContentClaim claim3 = cache.getContentClaim();
        assertEquals(claim1.getResourceClaim(), claim3.getResourceClaim());

        // Write 1 MB to the claim. This should result in the next Content Claim having a different Resource Claim.
        try (final OutputStream out = cache.write(claim3)) {
            assertNotNull(out);
            final byte[] buffer = new byte[1024 * 1024];
            final Random random = new Random();
            random.nextBytes(buffer);
            out.write(buffer);
        }

        assertEquals(3, claimManager.getClaimantCount(claim1.getResourceClaim()));

        final ContentClaim claim4 = cache.getContentClaim();
        assertNotNull(claim4);
        assertNotEquals(claim1.getResourceClaim(), claim4.getResourceClaim());

        assertEquals(1, claimManager.getClaimantCount(claim4.getResourceClaim()));
    }

    @Test
    public void testRolloverBoundaryDeterminedByContentRepository() throws IOException {
        final FileSystemRepository smallClaimRepository = createRepository(Map.of(NiFiProperties.MAX_APPENDABLE_CLAIM_SIZE, "1 KB"));
        assertEquals(1024L, smallClaimRepository.getMaxAppendableClaimBytes());

        final ContentClaimWriteCache cache = new StandardContentClaimWriteCache(smallClaimRepository, new NopPerformanceTracker(), CREATION_CONTEXT, BUFFER_SIZE);
        final byte[] content = new byte[600];

        final ContentClaim claim1 = cache.getContentClaim();
        try (final OutputStream out = cache.write(claim1)) {
            out.write(content);
        }

        // 600 bytes have been written to the Resource Claim, which is below the 1 KB limit, so it remains appendable.
        final ContentClaim claim2 = cache.getContentClaim();
        assertEquals(claim1.getResourceClaim(), claim2.getResourceClaim());

        try (final OutputStream out = cache.write(claim2)) {
            out.write(content);
        }

        // 1,200 bytes have now been written to the Resource Claim, which exceeds the 1 KB limit.
        final ContentClaim claim3 = cache.getContentClaim();
        assertNotEquals(claim1.getResourceClaim(), claim3.getResourceClaim());
    }
}
