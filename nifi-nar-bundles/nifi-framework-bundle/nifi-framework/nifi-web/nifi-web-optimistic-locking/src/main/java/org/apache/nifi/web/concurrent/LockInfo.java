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

public class LockInfo {
    private final String versionId;
    private final int lockCount;
    private final LockMode lockMode;
    private final long expirationTime;

    public LockInfo(final String versionId, final LockMode lockMode, final int lockCount, final long expirationPeriod, final TimeUnit expirationUnit) {
        this.versionId = versionId;
        this.lockMode = lockMode;
        this.lockCount = lockCount;
        this.expirationTime = System.nanoTime() + expirationUnit.toNanos(expirationPeriod);
    }

    public boolean isExpired() {
        return System.nanoTime() > expirationTime;
    }

    public String getVersionId() {
        return versionId;
    }

    public int getLockCount() {
        return lockCount;
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    @Override
    public String toString() {
        return "LockInfo[versionId=" + versionId + ", lockMode=" + lockMode + ", lockCount = " + lockCount + ", expired=" + isExpired() + "]";
    }
}
