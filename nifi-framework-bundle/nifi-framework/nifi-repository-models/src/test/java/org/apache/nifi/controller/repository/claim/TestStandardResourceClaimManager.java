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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStandardResourceClaimManager {

    @Timeout(10)
    @Test
    public void testGetClaimantCountWhileMarkingDestructable() throws InterruptedException, ExecutionException {
        final StandardResourceClaimManager manager = new StandardResourceClaimManager();

        for (int i = 0; i < 50000; i++) {
            final ResourceClaim rc = manager.newResourceClaim("container", "section", String.valueOf(i), false, false);
            manager.markDestructable(rc);
        }

        final Object completedObject = new Object();
        final CompletableFuture<Object> future = new CompletableFuture<>();
        final ResourceClaim lastClaim = manager.newResourceClaim("container", "section", "lastOne", false, false);
        final Thread backgroundThread = new Thread(() -> {
            manager.markDestructable(lastClaim);
            future.complete(completedObject);
        });

        backgroundThread.start();

        Thread.sleep(10);
        assertEquals(0, manager.getClaimantCount(lastClaim));
        assertNull(future.getNow(null));
        manager.drainDestructableClaims(new ArrayList<>(), 1);
        assertSame(completedObject, future.get());
    }

    @Test
    public void testMarkTruncatableSkipsDestructableResourceClaim() {
        final StandardResourceClaimManager manager = new StandardResourceClaimManager();

        // Create a resource claim with claimant count 0 and mark it destructable
        final ResourceClaim rc = manager.newResourceClaim("container", "section", "id1", false, false);
        manager.markDestructable(rc);

        // Create a content claim on that resource claim
        final StandardContentClaim contentClaim = new StandardContentClaim(rc, 0);
        contentClaim.setLength(1024);
        contentClaim.setTruncationCandidate(true);

        // markTruncatable should skip this because the resource claim is already destructable
        manager.markTruncatable(contentClaim);

        // Drain truncatable claims - should be empty
        final List<ContentClaim> truncated = new ArrayList<>();
        manager.drainTruncatableClaims(truncated, 10);
        assertTrue(truncated.isEmpty(), "Truncatable claims should be empty because the resource claim is destructable");
    }

    @Test
    public void testMarkTruncatableAndDrainRespectsMaxElements() {
        final StandardResourceClaimManager manager = new StandardResourceClaimManager();

        // Create 5 truncatable claims, each on a distinct resource claim with a positive claimant count
        for (int i = 0; i < 5; i++) {
            final ResourceClaim rc = manager.newResourceClaim("container", "section", "id-" + i, false, false);
            // Give each resource claim a positive claimant count so it's not destructable
            manager.incrementClaimantCount(rc);

            final StandardContentClaim cc = new StandardContentClaim(rc, 0);
            cc.setLength(1024);
            cc.setTruncationCandidate(true);
            manager.markTruncatable(cc);
        }

        // Drain with maxElements=3
        final List<ContentClaim> batch1 = new ArrayList<>();
        manager.drainTruncatableClaims(batch1, 3);
        assertEquals(3, batch1.size(), "First drain should return exactly 3 claims");

        // Drain again - should get remaining 2
        final List<ContentClaim> batch2 = new ArrayList<>();
        manager.drainTruncatableClaims(batch2, 10);
        assertEquals(2, batch2.size(), "Second drain should return the remaining 2 claims");
    }
}
