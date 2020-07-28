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

package org.apache.nifi.controller.swap;

import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.stream.io.NullOutputStream;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("deprecation")
public class TestSimpleSwapSerializerDeserializer {
    @Before
    public void setup() {
        MockFlowFile.resetIdGenerator();
    }

    @Test
    public void testRoundTripSerializeDeserialize() throws IOException {
        final ResourceClaimManager resourceClaimManager = new StandardResourceClaimManager();

        final List<FlowFileRecord> toSwap = new ArrayList<>(10000);
        final Map<String, String> attrs = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            attrs.put("i", String.valueOf(i));
            final FlowFileRecord ff = new MockFlowFile(attrs, i, resourceClaimManager);
            toSwap.add(ff);
        }

        final String queueId = "87bb99fe-412c-49f6-a441-d1b0af4e20b4";
        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        Mockito.when(flowFileQueue.getIdentifier()).thenReturn(queueId);

        final String swapLocation = "target/testRoundTrip-" + queueId + ".swap";
        final File swapFile = new File(swapLocation);

        Files.deleteIfExists(swapFile.toPath());
        try {
            final SimpleSwapSerializer serializer = new SimpleSwapSerializer();
            try (final FileOutputStream fos = new FileOutputStream(swapFile)) {
                serializer.serializeFlowFiles(toSwap, flowFileQueue, swapLocation, fos);
            }

            final SimpleSwapDeserializer deserializer = new SimpleSwapDeserializer();
            final SwapContents swappedIn;
            try (final FileInputStream fis = new FileInputStream(swapFile);
                final DataInputStream dis = new DataInputStream(fis)) {
                swappedIn = deserializer.deserializeFlowFiles(dis, swapLocation, flowFileQueue, resourceClaimManager);
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
        } finally {
            Files.deleteIfExists(swapFile.toPath());
        }
    }

    @Test
    @Ignore("For manual testing only. Not intended to be run as part of the automated unit tests but can "
        + "be convenient for determining a baseline for performance if making modifications.")
    public void testWritePerformance() throws IOException, InterruptedException {
        final ResourceClaimManager resourceClaimManager = new StandardResourceClaimManager();

        final List<FlowFileRecord> toSwap = new ArrayList<>(10000);
        final Map<String, String> attrs = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            attrs.put("i", String.valueOf(i));
            final FlowFileRecord ff = new MockFlowFile(attrs, i, resourceClaimManager);
            toSwap.add(ff);
        }

        final FlowFileQueue flowFileQueue = Mockito.mock(FlowFileQueue.class);
        Mockito.when(flowFileQueue.getIdentifier()).thenReturn("87bb99fe-412c-49f6-a441-d1b0af4e20b4");

        final String swapLocation = "target/testRoundTrip.swap";

        final int iterations = 1000;

        final long start = System.nanoTime();
        final SwapSerializer serializer = new SimpleSwapSerializer();
        for (int i = 0; i < iterations; i++) {
            try (final OutputStream out = new NullOutputStream()) {
                serializer.serializeFlowFiles(toSwap, flowFileQueue, swapLocation, out);
            }
        }

        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.println("Wrote " + iterations + " Swap Files in " + millis + " millis");
    }

}
