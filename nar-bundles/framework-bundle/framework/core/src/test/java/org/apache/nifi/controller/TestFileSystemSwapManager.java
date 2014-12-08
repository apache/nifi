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

import org.apache.nifi.controller.FileSystemSwapManager;
import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ContentClaimManager;

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

            final List<FlowFileRecord> records = FileSystemSwapManager.deserializeFlowFiles(in, flowFileQueue, new NopContentClaimManager());
            assertEquals(10000, records.size());

            for (final FlowFileRecord record : records) {
                assertEquals(4, record.getAttributes().size());
                assertEquals("value", record.getAttribute("key"));
            }
        }
    }

    public class NopContentClaimManager implements ContentClaimManager {

        @Override
        public ContentClaim newContentClaim(String container, String section, String id, boolean lossTolerant) {
            return null;
        }

        @Override
        public int getClaimantCount(ContentClaim claim) {
            return 0;
        }

        @Override
        public int decrementClaimantCount(ContentClaim claim) {
            return 0;
        }

        @Override
        public int incrementClaimantCount(ContentClaim claim) {
            return 0;
        }

        @Override
        public int incrementClaimantCount(ContentClaim claim, boolean newClaim) {
            return 0;
        }

        @Override
        public void markDestructable(ContentClaim claim) {
        }

        @Override
        public void drainDestructableClaims(Collection<ContentClaim> destination, int maxElements) {
        }

        @Override
        public void drainDestructableClaims(Collection<ContentClaim> destination, int maxElements, long timeout, TimeUnit unit) {
        }

        @Override
        public void purge() {
        }
    }
}
