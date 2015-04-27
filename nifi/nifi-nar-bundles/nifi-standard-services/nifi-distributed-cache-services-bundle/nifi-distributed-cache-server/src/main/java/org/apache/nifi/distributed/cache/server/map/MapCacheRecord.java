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

import java.nio.ByteBuffer;

import org.apache.nifi.distributed.cache.server.CacheRecord;

public class MapCacheRecord extends CacheRecord {

    private final ByteBuffer key;
    private final ByteBuffer value;

    public MapCacheRecord(final ByteBuffer key, final ByteBuffer value) {
        this.key = key;
        this.value = value;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return 2938476 + key.hashCode() * value.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj instanceof MapCacheRecord) {
            final MapCacheRecord that = ((MapCacheRecord) obj);
            return key.equals(that.key) && value.equals(that.value);
        }

        return false;
    }
}
