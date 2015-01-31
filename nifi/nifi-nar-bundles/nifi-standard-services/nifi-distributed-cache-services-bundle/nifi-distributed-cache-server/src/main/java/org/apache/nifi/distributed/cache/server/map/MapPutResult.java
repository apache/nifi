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

public class MapPutResult {
    private final boolean successful;
    private final ByteBuffer key, value;
    private final ByteBuffer existingValue;
    private final ByteBuffer evictedKey, evictedValue;
    
    public MapPutResult(final boolean successful, final ByteBuffer key, final ByteBuffer value, final ByteBuffer existingValue, final ByteBuffer evictedKey, final ByteBuffer evictedValue) {
        this.successful = successful;
        this.key = key;
        this.value = value;
        this.existingValue = existingValue;
        this.evictedKey = evictedKey;
        this.evictedValue = evictedValue;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }
    
    public ByteBuffer getExistingValue() {
        return existingValue;
    }

    public ByteBuffer getEvictedKey() {
        return evictedKey;
    }

    public ByteBuffer getEvictedValue() {
        return evictedValue;
    }
}
