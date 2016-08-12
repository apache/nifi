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

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.flowfile.FlowFile;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFileSystemSwapManager {

    @Test
    public void testBackwardCompatible() throws IOException {
        System.setProperty("nifi.properties.file.path", "src/test/resources/nifi.properties");

        try (final InputStream fis = new FileInputStream(new File("src/test/resources/old-swap-file.swap"));
            final DataInputStream in = new DataInputStream(new BufferedInputStream(fis))) {

            final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
            Mockito.when(flowFileQueue.getIdentifier()).thenReturn("87bb99fe-412c-49f6-a441-d1b0af4e20b4");

            final SwapContents swapContents = FileSystemSwapManager.deserializeFlowFiles(in, "/src/test/resources/old-swap-file.swap", flowFileQueue, new NopResourceClaimManager());
            final List<FlowFileRecord> records = swapContents.getFlowFiles();
            assertEquals(10000, records.size());

            for (final FlowFileRecord record : records) {
                assertEquals(4, record.getAttributes().size());
                assertEquals("value", record.getAttribute("key"));
            }
        }
    }

    @Test
    public void testRoundTripSerializeDeserialize() throws IOException {
        final List<FlowFileRecord> toSwap = new ArrayList<>(10000);
        final Map<String, String> attrs = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            attrs.put("i", String.valueOf(i));
            final FlowFileRecord ff = new TestFlowFile(attrs, i);
            toSwap.add(ff);
        }

        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        Mockito.when(flowFileQueue.getIdentifier()).thenReturn("87bb99fe-412c-49f6-a441-d1b0af4e20b4");

        final String swapLocation = "target/testRoundTrip.swap";
        final File swapFile = new File(swapLocation);
        Files.deleteIfExists(swapFile.toPath());

        try (final FileOutputStream fos = new FileOutputStream(swapFile)) {
            FileSystemSwapManager.serializeFlowFiles(toSwap, flowFileQueue, swapLocation, fos);
        }

        final SwapContents swappedIn;
        try (final FileInputStream fis = new FileInputStream(swapFile);
            final DataInputStream dis = new DataInputStream(fis)) {
            swappedIn = FileSystemSwapManager.deserializeFlowFiles(dis, swapLocation, flowFileQueue, Mockito.mock(ResourceClaimManager.class));
        }

        assertEquals(toSwap.size(), swappedIn.getFlowFiles().size());
        for (int i = 0; i < toSwap.size(); i++) {
            final FlowFileRecord pre = toSwap.get(i);
            final FlowFileRecord post = swappedIn.getFlowFiles().get(i);

            assertEquals(pre.getSize(), post.getSize());
            assertEquals(pre.getAttributes(), post.getAttributes());
            assertEquals(pre.getSize(), post.getSize());
            assertEquals(pre.getId(), post.getId());
            assertEquals(pre.getContentClaim(), post.getContentClaim());
            assertEquals(pre.getContentClaimOffset(), post.getContentClaimOffset());
            assertEquals(pre.getEntryDate(), post.getEntryDate());
            assertEquals(pre.getLastQueueDate(), post.getLastQueueDate());
            assertEquals(pre.getLineageStartDate(), post.getLineageStartDate());
            assertEquals(pre.getPenaltyExpirationMillis(), post.getPenaltyExpirationMillis());
        }
    }


    public class NopResourceClaimManager implements ResourceClaimManager {

        @Override
        public ResourceClaim newResourceClaim(String container, String section, String id, boolean lossTolerant) {
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


    private static class TestFlowFile implements FlowFileRecord {
        private static final AtomicLong idGenerator = new AtomicLong(0L);

        private final long id = idGenerator.getAndIncrement();
        private final long entryDate = System.currentTimeMillis();
        private final long lastQueueDate = System.currentTimeMillis();
        private final Map<String, String> attributes;
        private final long size;


        public TestFlowFile(final Map<String, String> attributes, final long size) {
            this.attributes = attributes;
            this.size = size;
        }


        @Override
        public long getId() {
            return id;
        }

        @Override
        public long getEntryDate() {
            return entryDate;
        }

        @Override
        public long getLineageStartDate() {
            return entryDate;
        }

        @Override
        public Long getLastQueueDate() {
            return lastQueueDate;
        }

        @Override
        public boolean isPenalized() {
            return false;
        }

        @Override
        public String getAttribute(String key) {
            return attributes.get(key);
        }

        @Override
        public long getSize() {
            return size;
        }

        @Override
        public Map<String, String> getAttributes() {
            return Collections.unmodifiableMap(attributes);
        }

        @Override
        public int compareTo(final FlowFile o) {
            return Long.compare(id, o.getId());
        }

        @Override
        public long getPenaltyExpirationMillis() {
            return -1L;
        }

        @Override
        public ContentClaim getContentClaim() {
            return null;
        }

        @Override
        public long getContentClaimOffset() {
            return 0;
        }

        @Override
        public long getLineageStartIndex() {
            return 0;
        }

        @Override
        public long getQueueDateIndex() {
            return 0;
        }
    }
}
