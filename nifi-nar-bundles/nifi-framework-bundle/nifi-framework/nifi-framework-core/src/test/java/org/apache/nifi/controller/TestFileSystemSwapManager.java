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
package org.apache.nifi.controller;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapManagerInitializationContext;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.stream.io.StreamUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class TestFileSystemSwapManager {

    @Test
    public void testBackwardCompatible() throws IOException {

        try (final InputStream fis = new FileInputStream(new File("src/test/resources/old-swap-file.swap"));
                final DataInputStream in = new DataInputStream(new BufferedInputStream(fis))) {

            final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
            when(flowFileQueue.getIdentifier()).thenReturn("87bb99fe-412c-49f6-a441-d1b0af4e20b4");

            final FileSystemSwapManager swapManager = createSwapManager();
            final SwapContents swapContents = swapManager.peek("src/test/resources/old-swap-file.swap", flowFileQueue);

            final List<FlowFileRecord> records = swapContents.getFlowFiles();
            assertEquals(10000, records.size());

            for (final FlowFileRecord record : records) {
                assertEquals(4, record.getAttributes().size());
                assertEquals("value", record.getAttribute("key"));
            }
        }
    }

    @Test
    public void testFailureOnRepoSwapOut() throws IOException {
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        when(flowFileQueue.getIdentifier()).thenReturn("87bb99fe-412c-49f6-a441-d1b0af4e20b4");

        final FlowFileRepository flowFileRepo = Mockito.mock(FlowFileRepository.class);
        Mockito.doThrow(new IOException("Intentional IOException for unit test"))
            .when(flowFileRepo).updateRepository(anyCollection());

        final FileSystemSwapManager swapManager = createSwapManager();

        final List<FlowFileRecord> flowFileRecords = new ArrayList<>();
        for (int i=0; i < 10000; i++) {
            flowFileRecords.add(new MockFlowFileRecord(i));
        }

        try {
            swapManager.swapOut(flowFileRecords, flowFileQueue, "partition-1");
            Assert.fail("Expected IOException");
        } catch (final IOException ioe) {
            // expected
        }
    }

    @Test
    public void testSwapFileUnknownToRepoNotSwappedIn() throws IOException {
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        when(flowFileQueue.getIdentifier()).thenReturn("");

        final File targetDir = new File("target/swap");
        targetDir.mkdirs();

        final File targetFile = new File(targetDir, "444-old-swap-file.swap");
        final File originalSwapFile = new File("src/test/resources/swap/444-old-swap-file.swap");
        try (final OutputStream fos = new FileOutputStream(targetFile);
             final InputStream fis = new FileInputStream(originalSwapFile)) {
            StreamUtils.copy(fis, fos);
        }

        final FileSystemSwapManager swapManager = new FileSystemSwapManager(Paths.get("target"));
        final ResourceClaimManager resourceClaimManager = new NopResourceClaimManager();
        final FlowFileRepository flowFileRepo = Mockito.mock(FlowFileRepository.class);

        swapManager.initialize(new SwapManagerInitializationContext() {
            @Override
            public ResourceClaimManager getResourceClaimManager() {
                return resourceClaimManager;
            }

            @Override
            public FlowFileRepository getFlowFileRepository() {
                return flowFileRepo;
            }

            @Override
            public EventReporter getEventReporter() {
                return EventReporter.NO_OP;
            }
        });

        when(flowFileRepo.isValidSwapLocationSuffix(anyString())).thenReturn(false);
        final List<String> recoveredLocations = swapManager.recoverSwapLocations(flowFileQueue, null);
        assertEquals(1, recoveredLocations.size());

        final String firstLocation = recoveredLocations.get(0);
        final SwapContents emptyContents = swapManager.swapIn(firstLocation, flowFileQueue);
        assertEquals(0, emptyContents.getFlowFiles().size());

        when(flowFileRepo.isValidSwapLocationSuffix(anyString())).thenReturn(true);
        when(flowFileQueue.getIdentifier()).thenReturn("87bb99fe-412c-49f6-a441-d1b0af4e20b4");
        final SwapContents contents = swapManager.swapIn(firstLocation, flowFileQueue);
        assertEquals(10000, contents.getFlowFiles().size());
    }

    private FileSystemSwapManager createSwapManager() {
        final FlowFileRepository flowFileRepo = Mockito.mock(FlowFileRepository.class);
        return createSwapManager(flowFileRepo);
    }

    private FileSystemSwapManager createSwapManager(final FlowFileRepository flowFileRepo) {
        final FileSystemSwapManager swapManager = new FileSystemSwapManager();
        final ResourceClaimManager resourceClaimManager = new NopResourceClaimManager();
        swapManager.initialize(new SwapManagerInitializationContext() {
            @Override
            public ResourceClaimManager getResourceClaimManager() {
                return resourceClaimManager;
            }

            @Override
            public FlowFileRepository getFlowFileRepository() {
                return flowFileRepo;
            }

            @Override
            public EventReporter getEventReporter() {
                return EventReporter.NO_OP;
            }
        });

        return swapManager;
    }

    public class NopResourceClaimManager implements ResourceClaimManager {
        @Override
        public ResourceClaim newResourceClaim(String container, String section, String id, boolean lossTolerant, boolean writable) {
            return null;
        }

        @Override
        public ResourceClaim getResourceClaim(String container, String section, String id) {
            return null;
        }

        @Override
        public int getClaimantCount(ResourceClaim claim) {
            return 0;
        }

        @Override
        public int decrementClaimantCount(ResourceClaim claim) {
            return 0;
        }

        @Override
        public int incrementClaimantCount(ResourceClaim claim) {
            return 0;
        }

        @Override
        public int incrementClaimantCount(ResourceClaim claim, boolean newClaim) {
            return 0;
        }

        @Override
        public void markDestructable(ResourceClaim claim) {
        }

        @Override
        public void drainDestructableClaims(Collection<ResourceClaim> destination, int maxElements) {
        }

        @Override
        public void drainDestructableClaims(Collection<ResourceClaim> destination, int maxElements, long timeout, TimeUnit unit) {
        }

        @Override
        public void purge() {
        }

        @Override
        public void freeze(ResourceClaim claim) {
        }
    }

}
