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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
}
