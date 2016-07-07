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

import java.util.concurrent.TimeUnit;

import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;

public class DistributedReadWriteLock implements DistributedLockingManager {
    private final DistributedLock readLock;
    private final DistributedLock writeLock;

    public DistributedReadWriteLock(final NiFiProperties properties) {
        this(FormatUtils.getTimeDuration(properties.getProperty(NiFiProperties.REQUEST_REPLICATION_CLAIM_TIMEOUT,
            NiFiProperties.DEFAULT_REQUEST_REPLICATION_CLAIM_TIMEOUT), TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }

    public DistributedReadWriteLock(final long lockExpirationPeriod, final TimeUnit lockExpirationUnit) {
        final ReadWriteLockSync sync = new ReadWriteLockSync();
        readLock = new ReentrantDistributedLock(LockMode.SHARED, sync, lockExpirationPeriod, lockExpirationUnit);
        writeLock = new ReentrantDistributedLock(LockMode.MUTUALLY_EXCLUSIVE, sync, lockExpirationPeriod, lockExpirationUnit);
    }

    @Override
    public DistributedLock getReadLock() {
        return readLock;
    }

    @Override
    public DistributedLock getWriteLock() {
        return writeLock;
    }
}
