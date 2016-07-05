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

package org.apache.nifi.web.concurrent;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReentrantDistributedLock implements DistributedLock {
    private static final Logger logger = LoggerFactory.getLogger(ReentrantDistributedLock.class);

    private final long expirationNanos;

    private final ReadWriteLockSync sync;
    private final LockMode lockMode;

    public ReentrantDistributedLock(final LockMode lockMode, final ReadWriteLockSync sync, final long expirationTimePeriod, final TimeUnit expirationTimeUnit) {
        this.lockMode = lockMode;
        this.sync = sync;
        this.expirationNanos = expirationTimeUnit.toNanos(expirationTimePeriod);
    }

    int getClaimCount() {
        final LockInfo currentInfo = sync.get();
        if (currentInfo == null || currentInfo.isExpired()) {
            return 0;
        }

        return currentInfo.getLockCount();
    }

    @Override
    public String lock() {
        return lock(null);
    }

    @Override
    public String lock(final String versionIdentifier) {
        return tryLock(-1L, TimeUnit.MILLISECONDS, versionIdentifier);
    }

    @Override
    public String tryLock(final long time, final TimeUnit timeUnit) {
        return tryLock(time, timeUnit, null);
    }

    @Override
    public String tryLock(final long timePeriod, final TimeUnit timeUnit, final String versionIdentifier) {
        final long stopTryingTime = timePeriod < 0 ? -1L : System.nanoTime() + timeUnit.toNanos(timePeriod);
        logger.debug("Attempting to obtain {} lock with a max wait of {} {}", lockMode, timePeriod, timeUnit);

        long i = 0;
        while (true) {
            if (i++ > 0) {
                if (stopTryingTime > 0L && System.nanoTime() > stopTryingTime) {
                    logger.debug("Failed to obtain {} lock within {} {}; returning null for tryLock", lockMode, timePeriod, timeUnit);
                    return null;
                }

                // If not the first time we've reached this point, we want to
                // give other threads a chance to release their locks before
                // we enter the synchronized block.
                Thread.yield();
            }

            synchronized (sync) {
                final LockInfo currentInfo = sync.get();
                logger.trace("Current Lock Info = {}", currentInfo);

                if (currentInfo == null || currentInfo.isExpired()) {
                    // There is no lock currently held. Attempt to obtain the lock.
                    final String versionId = versionIdentifier == null ? UUID.randomUUID().toString() : versionIdentifier;
                    final boolean updated = updateLockInfo(currentInfo, versionId, 1);

                    if (updated) {
                        // Lock has been obtained. Return the current version.
                        logger.debug("Obtained {} lock with Version ID {}", lockMode, versionId);
                        return versionId;
                    } else {
                        // Try again.
                        logger.debug("Failed to update atomic reference. Trying again");
                        continue;
                    }
                } else {
                    // There is already a lock held. If the lock that is being held is SHARED,
                    // and this is a SHARED lock, then we can use it.
                    if (lockMode == LockMode.SHARED && currentInfo.getLockMode() == LockMode.SHARED) {
                        logger.debug("Lock is already held but is a shared lock. Attempting to increment lock count");

                        // lock being held is a shared lock, and this is a shared lock. We can just
                        // update the Lock Info by incrementing the lock count and using a new expiration time.
                        final boolean updated = updateLockInfo(currentInfo, currentInfo.getVersionId(), currentInfo.getLockCount() + 1);
                        if (updated) {
                            // lock info was updated. Return the current version.
                            logger.debug("Incremented lock count. Obtained {} lock with Version ID {}", lockMode, currentInfo.getVersionId());
                            return currentInfo.getVersionId();
                        } else {
                            // failed to update the lock info. The lock has expired, so we have to start over.
                            logger.debug("Failed to update atomic reference. Trying again");
                            continue;
                        }
                    } else {
                        // either the lock being held is a mutex or this lock requires a mutex. Either
                        // way, we cannot enter the lock, so we will wait a bit and then retry.
                        // We wait before entering synchronized block, because we don't want to overuse
                        // the CPU and we want to give other threads a chance to unlock the lock.
                        logger.debug("Cannot obtain {} lock because it is already held and cannot be shared. Trying again", lockMode);
                        continue;
                    }
                }
            }
        }
    }

    protected boolean updateLockInfo(final LockInfo currentInfo, final String versionId, final int lockCount) {
        final LockInfo newInfo = new LockInfo(versionId, lockMode, lockCount, expirationNanos, TimeUnit.NANOSECONDS);
        return sync.update(currentInfo, newInfo);
    }

    @Override
    public <T> T withLock(final String identifier, final Supplier<T> action) throws LockExpiredException {
        synchronized (sync) {
            verifyIdentifier(identifier, sync.get());
            return action.get();
        }
    }

    @Override
    public void unlock(final String identifier) throws LockExpiredException {
        synchronized (sync) {
            final LockInfo info = sync.get();
            verifyIdentifier(identifier, info);

            final int newLockCount = info.getLockCount() - 1;
            if (newLockCount <= 0) {
                sync.update(info, null);
            } else {
                sync.update(info, new LockInfo(info.getVersionId(), lockMode, newLockCount, expirationNanos, TimeUnit.NANOSECONDS));
            }
        }
    }

    private void verifyIdentifier(final String identifier, final LockInfo lockInfo) throws LockExpiredException {
        if (lockInfo == null) {
            throw new LockExpiredException("No lock has been obtained");
        }

        if (!lockInfo.getVersionId().equals(identifier)) {
            throw new LockExpiredException("Incorrect Lock ID provided. This typically means that the lock has already expired and another lock has been obtained.");
        }

        if (lockInfo.isExpired()) {
            throw new LockExpiredException("Lock has already expired");
        }
    }
}
