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

    private static final ConcurrentMap<ResourceClaim, AtomicInteger> claimantCounts = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(StandardResourceClaimManager.class);

    private static final BlockingQueue<ResourceClaim> destructableClaims = new LinkedBlockingQueue<>(50000);

    @Override
    public ResourceClaim newResourceClaim(final String container, final String section, final String id, final boolean lossTolerant) {
        return new StandardResourceClaim(this, container, section, id, lossTolerant);
    }

    private static AtomicInteger getCounter(final ResourceClaim claim) {
        if (claim == null) {
            return null;
        }

        AtomicInteger counter = claimantCounts.get(claim);
        if (counter != null) {
            return counter;
        }

        counter = new AtomicInteger(0);
        final AtomicInteger existingCounter = claimantCounts.putIfAbsent(claim, counter);
        return existingCounter == null ? counter : existingCounter;
    }

    @Override
    public int getClaimantCount(final ResourceClaim claim) {
        if (claim == null) {
            return 0;
        }

        synchronized (claim) {
            final AtomicInteger counter = claimantCounts.get(claim);
            return counter == null ? 0 : counter.get();
        }
    }

    @Override
    public int decrementClaimantCount(final ResourceClaim claim) {
        if (claim == null) {
            return 0;
        }

        synchronized (claim) {
            final AtomicInteger counter = claimantCounts.get(claim);
            if (counter == null) {
                logger.warn("Decrementing claimant count for {} but claimant count is not known. Returning -1", claim);
                return -1;
            }

            final int newClaimantCount = counter.decrementAndGet();
            if (newClaimantCount < 0) {
                logger.error("Decremented claimant count for {} to {}", claim, newClaimantCount);
            } else {
                logger.debug("Decrementing claimant count for {} to {}", claim, newClaimantCount);
            }

            if (newClaimantCount == 0) {
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
    }
}
