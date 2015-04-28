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
package org.apache.nifi.distributed.cache.server;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CacheRecord {

    private static final AtomicLong idGenerator = new AtomicLong(0L);

    private final long id;
    private final long entryDate;
    private volatile long lastHitDate;
    private final AtomicInteger hitCount = new AtomicInteger(0);

    public CacheRecord() {
        entryDate = System.currentTimeMillis();
        lastHitDate = entryDate;
        id = idGenerator.getAndIncrement();
    }

    public long getEntryDate() {
        return entryDate;
    }

    public long getLastHitDate() {
        return lastHitDate;
    }

    public int getHitCount() {
        return hitCount.get();
    }

    public void hit() {
        hitCount.getAndIncrement();
        lastHitDate = System.currentTimeMillis();
    }

    public long getId() {
        return id;
    }
}
