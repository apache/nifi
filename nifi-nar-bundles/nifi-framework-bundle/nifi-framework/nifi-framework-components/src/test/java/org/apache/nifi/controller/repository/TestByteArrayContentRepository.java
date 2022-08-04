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
package org.apache.nifi.controller.repository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestByteArrayContentRepository {

    private ResourceClaimManager claimManager;

    @Before
    public void setup() {
        claimManager = new StandardResourceClaimManager();
    }

    @Test
    public void testCreateUniqueClaim() throws IOException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestByteArrayContentRepository.class.getResource("/conf/nifi.properties").getFile());
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(ByteArrayContentRepository.MAX_SIZE_PROPERTY, "11 MB");
        final NiFiProperties nifiProps = NiFiProperties.createBasicNiFiProperties(null, addProps);
        final ByteArrayContentRepository contentRepo = new ByteArrayContentRepository(nifiProps);
        final TestContentRepositoryContext contextRepo = new TestContentRepositoryContext(claimManager, EventReporter.NO_OP);

        contentRepo.initialize(contextRepo);
        final ByteArrayContentRepository.ByteArrayContentClaim claim1 = ByteArrayContentRepository.verifyContentClaim(contentRepo.create(true));
        final ByteArrayContentRepository.ByteArrayContentClaim claim2 = ByteArrayContentRepository.verifyContentClaim(contentRepo.create(true));

        Assert.assertTrue(claim1.equals(claim1));
        Assert.assertFalse(claim1.equals(claim2));
    }

    @Test
    public void testRedirects() throws IOException {
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(ByteArrayContentRepository.MAX_SIZE_PROPERTY, "10 MB");
        final NiFiProperties nifiProps = NiFiProperties.createBasicNiFiProperties(null, addProps);
        final TestContentRepositoryContext contextRepo = new TestContentRepositoryContext(claimManager, EventReporter.NO_OP);
        final ByteArrayContentRepository contentRepo = new ByteArrayContentRepository(nifiProps);
        contentRepo.initialize(contextRepo);
        final ContentClaim claim = contentRepo.create(true);
        final OutputStream out = contentRepo.write(claim);

        final byte[] oneK = new byte[1024];
        Arrays.fill(oneK, (byte) 55);

        // Write 10 MB to the repo
        for (int i = 0; i < 10240; i++) {
            out.write(oneK);
        }

        final long currentUsableSpaceExpectedZero = contentRepo.getContainerUsableSpace(ByteArrayContentRepository.CONTAINER_NAME);
        Assert.assertEquals(currentUsableSpaceExpectedZero, 0L);

        try {
            out.write(1);
            Assert.fail("Expected to be out of space on content repo");
        } catch (final OutOfMemoryError e) {
        }

        try {
            out.write(1);
            Assert.fail("Expected to be out of space on content repo");
        } catch (final OutOfMemoryError e) {
        }
    }

    @Test
    public void testMemoryIsFreed() throws IOException, InterruptedException, ExecutionException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestByteArrayContentRepository.class.getResource("/conf/nifi.properties").getFile());
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(ByteArrayContentRepository.MAX_SIZE_PROPERTY, "11 MB");
        final NiFiProperties nifiProps = NiFiProperties.createBasicNiFiProperties(null, addProps);
        final ByteArrayContentRepository contentRepo = new ByteArrayContentRepository(nifiProps);
        final TestContentRepositoryContext contextRepo = new TestContentRepositoryContext(claimManager, EventReporter.NO_OP);

        contentRepo.initialize(contextRepo);

        final byte[] oneK = new byte[1024];
        Arrays.fill(oneK, (byte) 55);

        final ExecutorService exec = Executors.newFixedThreadPool(10);

        try {
            for (int t = 0; t < 10; t++) {
                final Callable writeAndRemoveToRepo = new Callable<Long>() {
                    @Override
                    public Long call() throws IOException, ExecutionException {
                        for (int j = 0; j < 10000; j++) {
                            final ContentClaim claim = contentRepo.create(true);
                            final OutputStream out = contentRepo.write(claim);
        
                            // Write 1 MB to the repo
                            for (int i = 0; i < 1024; i++) {
                                out.write(oneK);
                            }
        
                            final int count = contentRepo.decrementClaimantCount(claim);
                            if (count <= 0) {
                                contentRepo.remove(claim);
                            }
                        }
                        return Long.valueOf(contentRepo.getContainerUsableSpace(ByteArrayContentRepository.CONTAINER_NAME));
                    }
                };
                Future<Long> future = exec.submit(writeAndRemoveToRepo);
                Long currentSize = future.get();
            }
        } finally {
            exec.shutdown();
            exec.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSimpleReadWrite() throws IOException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestByteArrayContentRepository.class.getResource("/conf/nifi.properties").getFile());
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(ByteArrayContentRepository.MAX_SIZE_PROPERTY, "11 MB");
        final NiFiProperties nifiProps = NiFiProperties.createBasicNiFiProperties(null, addProps);
        final ByteArrayContentRepository contentRepo = new ByteArrayContentRepository(nifiProps);
        final TestContentRepositoryContext contextRepo = new TestContentRepositoryContext(claimManager, EventReporter.NO_OP);

        contentRepo.initialize(contextRepo);
        final ContentClaim claim = contentRepo.create(true);

        final int byteCount = 2398473 * 4;
        final byte[] x = new byte[4];
        x[0] = 48;
        x[1] = 29;
        x[2] = 49;
        x[3] = 51;

        try (final OutputStream out = contentRepo.write(claim);) {
            for (int i = 0; i < byteCount / 4; i++) {
                out.write(x);
            }
        }

        final InputStream in = contentRepo.read(claim);
        for (int i = 0; i < byteCount; i++) {
            final int val = in.read();
            final int index = i % 4;
            final byte expectedVal = x[index];
            assertEquals(expectedVal, val);
        }

        assertEquals(-1, in.read());
    }

    @Test
    public void testReadWriteWithOffset() throws IOException {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, TestByteArrayContentRepository.class.getResource("/conf/nifi.properties").getFile());
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(ByteArrayContentRepository.MAX_SIZE_PROPERTY, "11 MB");
        final NiFiProperties nifiProps = NiFiProperties.createBasicNiFiProperties(null, addProps);
        final ByteArrayContentRepository contentRepo = new ByteArrayContentRepository(nifiProps);
        final TestContentRepositoryContext contextRepo = new TestContentRepositoryContext(claimManager, EventReporter.NO_OP);

        contentRepo.initialize(contextRepo);
        final ContentClaim claim = contentRepo.create(true);

        final String string_1 = "[{\"test\": \"ouais\"}]";
        final String string_2 = "[{\"test\": \"non\"}]";

        final byte[] bytes_1 = string_1.getBytes(Charset.forName("UTF-8"));
        final byte[] bytes_2 = string_2.getBytes(Charset.forName("UTF-8"));

        try (final OutputStream out = contentRepo.write(claim);) {
            out.write(bytes_1);
            out.write(bytes_2);
        }

        assertEquals(contentRepo.size(claim), bytes_1.length + bytes_2.length);

        try (final ByteArrayOutputStream bout = new ByteArrayOutputStream();) {
            final long length_1 = contentRepo.exportTo(claim, bout, 0L, bytes_1.length);
            assertEquals(length_1, bytes_1.length);
            assertEquals(bout.toString(Charset.forName("UTF-8")), string_1);
        }

        try (final ByteArrayOutputStream bout = new ByteArrayOutputStream();) {
            final long length_2 = contentRepo.exportTo(claim, bout, bytes_1.length, bytes_2.length);
            assertEquals(length_2, bytes_2.length);
            assertEquals(bout.toString(Charset.forName("UTF-8")), string_2);
        }

        try (final ByteArrayOutputStream bout = new ByteArrayOutputStream();) {
            final long length_3 = contentRepo.exportTo(claim, bout);
            assertEquals(length_3, bytes_1.length + bytes_2.length);
            assertEquals(bout.toString(Charset.forName("UTF-8")), string_1 + string_2);
        }
    }

    public class TestContentRepositoryContext implements ContentRepositoryContext {

        private final ResourceClaimManager resourceClaimManager;
        private final EventReporter eventReporter;
    
        public TestContentRepositoryContext(ResourceClaimManager resourceClaimManager, EventReporter eventReporter) {
            this.resourceClaimManager = resourceClaimManager;
            this.eventReporter = eventReporter;
        }
    
        @Override
        public ResourceClaimManager getResourceClaimManager() {
            return resourceClaimManager;
        }
    
        @Override
        public EventReporter getEventReporter() {
            return eventReporter;
        }
    }
}
