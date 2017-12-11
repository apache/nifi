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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestStandardResourceClaimManager {


    @Test(timeout = 10000)
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
        assertTrue(completedObject == future.get());
    }


    @Test
    @Ignore("Unit test was created to repeat a concurrency bug in StandardResourceClaimManager. "
        + "However, now that the concurrency bug has been fixed, the test will deadlock. Leaving here for now in case it's valuable before the commit is pushed")
    public void testIncrementAndDecrementThreadSafety() throws InterruptedException {
        final AtomicBoolean waitToRemove = new AtomicBoolean(true);
        final CountDownLatch decrementComplete = new CountDownLatch(1);

        final StandardResourceClaimManager manager = new StandardResourceClaimManager() {
            @Override
            protected void removeClaimantCount(final ResourceClaim claim) {
                decrementComplete.countDown();

                while (waitToRemove.get()) {
                    try {
                        Thread.sleep(10L);
                    } catch (final InterruptedException ie) {
                        Assert.fail("Interrupted while waiting to remove claimant count");
                    }
                }

                super.removeClaimantCount(claim);
            }
        };

        final ResourceClaim resourceClaim = manager.newResourceClaim("container", "section", "id", false, false);
        assertEquals(1, manager.incrementClaimantCount(resourceClaim)); // increment claimant count to 1.

        assertEquals(1, manager.getClaimantCount(resourceClaim));

        // Decrement the claimant count. This should decrement the count to 0. However, we have 'waitToRemove' set to true,
        // so the manager will not actually remove the claimant count (or return from this method) until we set 'waitToRemove'
        // to false. We do this so that we can increment the claimant count in a separate thread. Because we will be incrementing
        // the count in 1 thread and decrementing it in another thread, the end result should be that the claimant count is still
        // at 1.
        final Runnable decrementCountRunnable = new Runnable() {
            @Override
            public void run() {
                manager.decrementClaimantCount(resourceClaim);
            }
        };

        final Runnable incrementCountRunnable = new Runnable() {
            @Override
            public void run() {
                // Wait until the count has been decremented
                try {
                    decrementComplete.await();
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail(e.toString());
                }

                // Increment the claimant count
                manager.incrementClaimantCount(resourceClaim);

                // allow the 'decrement Thread' to complete
                waitToRemove.set(false);
            }
        };

        // Start the threads so that the claim count is incremented and decremented at the same time
        final Thread decrementThread = new Thread(decrementCountRunnable);
        final Thread incrementThread = new Thread(incrementCountRunnable);

        decrementThread.start();
        incrementThread.start();

        // Wait for both threads to complete
        incrementThread.join();
        decrementThread.join();

        // claimant count should still be 1, since 1 thread incremented it and 1 thread decremented it!
        assertEquals(1, manager.getClaimantCount(resourceClaim));
    }

}
