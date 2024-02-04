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
package org.apache.nifi.distributed.cache.operations;

/**
 * Represents a distributed set cache operation which may be invoked.
 */
public enum MapOperation implements CacheOperation {
    CONTAINS_KEY("containsKey"),
    FETCH("fetch"),
    GET("get"),
    GET_AND_PUT_IF_ABSENT("getAndPutIfAbsent"),
    KEYSET("keySet"),
    PUT("put"),
    PUT_IF_ABSENT("putIfAbsent"),
    REMOVE("remove"),
    REMOVE_AND_GET("removeAndGet"),
    REPLACE("replace"),
    SUBMAP("subMap"),
    CLOSE("close");

    private final String operation;

    MapOperation(final String operation) {
        this.operation = operation;
    }

    @Override
    public String value() {
        return operation;
    }
}
