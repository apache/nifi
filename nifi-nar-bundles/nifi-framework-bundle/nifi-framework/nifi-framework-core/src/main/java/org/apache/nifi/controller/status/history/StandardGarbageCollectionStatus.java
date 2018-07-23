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

package org.apache.nifi.controller.status.history;

import java.util.Date;

public class StandardGarbageCollectionStatus implements GarbageCollectionStatus {
    private final String managerName;
    private final Date timestamp;
    private final long collectionCount;
    private final long collectionMillis;

    public StandardGarbageCollectionStatus(final String managerName, final Date timestamp, final long collectionCount, final long collectionMillis) {
        this.managerName = managerName;
        this.timestamp = timestamp;
        this.collectionCount = collectionCount;
        this.collectionMillis = collectionMillis;
    }

    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public String getMemoryManagerName() {
        return managerName;
    }

    @Override
    public long getCollectionCount() {
        return collectionCount;
    }

    @Override
    public long getCollectionMillis() {
        return collectionMillis;
    }
}
