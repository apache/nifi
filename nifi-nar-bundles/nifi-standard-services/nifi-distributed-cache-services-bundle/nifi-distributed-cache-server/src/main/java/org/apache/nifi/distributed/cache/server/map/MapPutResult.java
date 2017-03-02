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
package org.apache.nifi.distributed.cache.server.map;

public class MapPutResult {

    private final boolean successful;
    private final MapCacheRecord record;
    private final MapCacheRecord existing;
    private final MapCacheRecord evicted;

    public MapPutResult(boolean successful, MapCacheRecord record, MapCacheRecord existing, MapCacheRecord evicted) {
        this.successful = successful;
        this.record = record;
        this.existing = existing;
        this.evicted = evicted;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public MapCacheRecord getRecord() {
        return record;
    }

    public MapCacheRecord getExisting() {
        return existing;
    }

    public MapCacheRecord getEvicted() {
        return evicted;
    }
}
