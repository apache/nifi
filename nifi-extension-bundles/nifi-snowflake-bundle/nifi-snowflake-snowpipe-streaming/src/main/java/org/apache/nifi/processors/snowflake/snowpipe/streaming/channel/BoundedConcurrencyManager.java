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
package org.apache.nifi.processors.snowflake.snowpipe.streaming.channel;

import org.apache.nifi.logging.ComponentLog;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Concurrency Manager implementation with defined maximum concurrency
 */
public class BoundedConcurrencyManager implements ConcurrencyManager {
    private final ComponentLog logger;

    private final int maxConcurrency;

    private final Map<String, Integer> concurrencyGroups = new ConcurrentHashMap<>();

    public BoundedConcurrencyManager(final int maxConcurrency, final ComponentLog logger) {
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be greater than 0");
        }

        this.maxConcurrency = maxConcurrency;
        this.logger = Objects.requireNonNull(logger, "Component Log required");
    }

    @Override
    public ConcurrencyClaimResult tryClaim(final ConcurrencyClaimRequest request) {
        // If the thread has already been allocated a slot, we can just accept the claim.
        final ConcurrencyClaim claim = request.getExistingClaim();
        final String groupRequested = request.getGroupRequested();

        if (claim.isGroupAcquired(groupRequested)) {
            request.accept();

            logger.debug("Claim for Concurrency Group [{}] accepted because group already acquired by this claim", groupRequested);
            return ConcurrencyClaimResult.INHERITED;
        }

        boolean updated = false;
        while (!updated) {
            final Integer currentValue = concurrencyGroups.get(groupRequested);

            // Update value to 1 greater than the current value. We cannot just use concurrencyMap.replace() if currentValue is null
            // because ConcurrentHashMap doesn't support null values
            if (currentValue == null) {
                final Integer previousValue = concurrencyGroups.putIfAbsent(groupRequested, 1);
                updated = previousValue == null;

                if (updated) {
                    logger.debug("Concurrency for group [{}] increased from 0 to 1", groupRequested);
                }
            } else if (currentValue >= maxConcurrency) {
                request.reject();
                return ConcurrencyClaimResult.REJECTED;
            } else {
                updated = concurrencyGroups.replace(groupRequested, currentValue, currentValue + 1);

                if (updated) {
                    logger.debug("Concurrency for group [{}] increased from {} to {}", groupRequested, currentValue, currentValue + 1);
                }
            }
        }

        request.accept();
        return ConcurrencyClaimResult.OWNED;
    }

    @Override
    public void releaseClaim(final String concurrencyGroup) {
        if (concurrencyGroup == null) {
            return;
        }

        boolean updated = false;
        while (!updated) {
            final Integer currentValue = concurrencyGroups.get(concurrencyGroup);
            if (currentValue == null) {
                throw new IllegalArgumentException("Concurrency group does not exist: " + concurrencyGroup);
            }

            if (currentValue == 1) {
                updated = concurrencyGroups.remove(concurrencyGroup, currentValue);
            } else {
                updated = concurrencyGroups.replace(concurrencyGroup, currentValue, currentValue - 1);
            }

            if (updated) {
                logger.debug("Concurrency for group [{}] reduced from {} to {}", concurrencyGroup, currentValue, currentValue - 1);
            }
        }
    }

    @Override
    public boolean isEmpty() {
        return concurrencyGroups.isEmpty();
    }
}
