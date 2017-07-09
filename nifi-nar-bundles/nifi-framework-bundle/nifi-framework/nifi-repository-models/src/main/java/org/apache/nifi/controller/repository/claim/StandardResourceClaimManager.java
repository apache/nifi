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

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardResourceClaimManager implements ResourceClaimManager {

    private static final Logger logger = LoggerFactory.getLogger(StandardResourceClaimManager.class);
    private final ConcurrentMap<ResourceClaim, ClaimCount> claimantCounts = new ConcurrentHashMap<>();
    private final BlockingQueue<ResourceClaim> destructableClaims = new LinkedBlockingQueue<>(50000);

    @Override
    public ResourceClaim newResourceClaim(final String container, final String section, final String id, final boolean lossTolerant, final boolean writable) {
        final StandardResourceClaim claim = new StandardResourceClaim(this, container, section, id, lossTolerant);
        if (!writable) {
            claim.freeze();
        }
        return claim;
    }

    @Override
    public ResourceClaim getResourceClaim(final String container, final String section, final String id) {
        final ResourceClaim tempClaim = new StandardResourceClaim(this, container, section, id, false);
        final ClaimCount count = claimantCounts.get(tempClaim);
        return (count == null) ? null : count.getClaim();
    }

    private AtomicInteger getCounter(final ResourceClaim claim) {
        if (claim == null) {
            return null;
        }

        ClaimCount counter = claimantCounts.get(claim);
        if (counter != null) {
            return counter.getCount();
        }

        counter = new ClaimCount(claim, new AtomicInteger(0));
        final ClaimCount existingCounter = claimantCounts.putIfAbsent(claim, counter);
        return existingCounter == null ? counter.getCount() : existingCounter.getCount();
    }

    @Override
    public int getClaimantCount(final ResourceClaim claim) {
        if (claim == null) {
            return 0;
        }

        // No need to synchronize on the Resource Claim here, since this is simply obtaining a value.
        // We synchronize elsewhere because we want to atomically perform multiple operations, such as
        // getting the claimant count and then updating a queue. However, the operation of obtaining
        // the ClaimCount and getting its count value has no side effect and therefore can be performed
        // without synchronization (since the claimantCounts map and the ClaimCount are also both thread-safe
        // and there is no need for the two actions of obtaining the ClaimCount and getting its Count value
        // to be performed atomically).
        final ClaimCount counter = claimantCounts.get(claim);
        return counter == null ? 0 : counter.getCount().get();
    }

    @Override
    public int decrementClaimantCount(final ResourceClaim claim) {
        if (claim == null) {
            return 0;
        }

        synchronized (claim) {
            final ClaimCount counter = claimantCounts.get(claim);
            if (counter == null) {
                logger.warn("Decrementing claimant count for {} but claimant count is not known. Returning -1", claim);
                return -1;
            }

            final int newClaimantCount = counter.getCount().decrementAndGet();
            if (newClaimantCount < 0) {
                logger.error("Decremented claimant count for {} to {}", claim, newClaimantCount);
            } else {
                logger.debug("Decrementing claimant count for {} to {}", claim, newClaimantCount);
            }

            // If the claim is no longer referenced, we want to remove it. We consider the claim to be "no longer referenced"
            // if the count is 0 and it is no longer writable (if it's writable, it may still be writable by the Content Repository,
            // even though no existing FlowFile is referencing the claim).
            if (newClaimantCount == 0 && !claim.isWritable()) {
                removeClaimantCount(claim);
            }
            return newClaimantCount;
        }
    }

    // protected so that it can be used in unit tests
    protected void removeClaimantCount(final ResourceClaim claim) {
        claimantCounts.remove(claim);
    }

    @Override
    public int incrementClaimantCount(final ResourceClaim claim) {
        return incrementClaimantCount(claim, false);
    }

    @Override
    public int incrementClaimantCount(final ResourceClaim claim, final boolean newClaim) {
        if (claim == null) {
            return 0;
        }

        synchronized (claim) {
            final AtomicInteger counter = getCounter(claim);

            final int newClaimantCount = counter.incrementAndGet();
            logger.debug("Incrementing claimant count for {} to {}", claim, newClaimantCount);

            // If the claimant count moved from 0 to 1, remove it from the queue of destructable claims.
            if (!newClaim && newClaimantCount == 1) {
                destructableClaims.remove(claim);
            }
            return newClaimantCount;
        }
    }

    @Override
    public void markDestructable(final ResourceClaim claim) {
        if (claim == null) {
            return;
        }

        synchronized (claim) {
            if (getClaimantCount(claim) > 0) {
                return;
            }

            logger.debug("Marking claim {} as destructable", claim);
            try {
                while (!destructableClaims.offer(claim, 30, TimeUnit.MINUTES)) {
                }
            } catch (final InterruptedException ie) {
            }
        }
    }

    @Override
    public void drainDestructableClaims(final Collection<ResourceClaim> destination, final int maxElements) {
        final int drainedCount = destructableClaims.drainTo(destination, maxElements);
        logger.debug("Drained {} destructable claims to {}", drainedCount, destination);
    }

    @Override
    public void drainDestructableClaims(final Collection<ResourceClaim> destination, final int maxElements, final long timeout, final TimeUnit unit) {
        try {
            final ResourceClaim firstClaim = destructableClaims.poll(timeout, unit);
            if (firstClaim != null) {
                destination.add(firstClaim);
                destructableClaims.drainTo(destination, maxElements - 1);
            }
        } catch (final InterruptedException e) {
        }
    }

    @Override
    public void purge() {
        claimantCounts.clear();
    }

    @Override
    public void freeze(final ResourceClaim claim) {
        if (claim == null) {
            return;
        }

        if (!(claim instanceof StandardResourceClaim)) {
            throw new IllegalArgumentException("The given resource claim is not managed by this Resource Claim Manager");
        }

        ((StandardResourceClaim) claim).freeze();

        synchronized (claim) {
            if (getClaimantCount(claim) == 0) {
                claimantCounts.remove(claim);
            }
        }
    }


    private static final class ClaimCount {
        private final ResourceClaim claim;
        private final AtomicInteger count;

        public ClaimCount(final ResourceClaim claim, final AtomicInteger count) {
            this.claim = claim;
            this.count = count;
        }

        public AtomicInteger getCount() {
            return count;
        }

        public ResourceClaim getClaim() {
            return claim;
        }
    }
}
