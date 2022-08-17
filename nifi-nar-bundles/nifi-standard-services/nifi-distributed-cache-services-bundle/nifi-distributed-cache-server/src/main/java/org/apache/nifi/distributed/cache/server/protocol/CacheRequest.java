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
package org.apache.nifi.distributed.cache.server.protocol;

import org.apache.nifi.distributed.cache.operations.CacheOperation;

import java.util.Objects;

/**
 * Cache Request Packet
 */
public class CacheRequest {
    private final CacheOperation cacheOperation;

    private final byte[] body;

    public CacheRequest(
            final CacheOperation cacheOperation,
            final byte[] body
    ) {
        this.cacheOperation = Objects.requireNonNull(cacheOperation, "Cache Operation required");
        this.body = Objects.requireNonNull(body, "Body required");
    }

    public CacheOperation getCacheOperation() {
        return cacheOperation;
    }

    public byte[] getBody() {
        return body;
    }
}
