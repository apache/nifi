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
package org.apache.nifi.processor.util.list;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ListedEntity {
    /**
     * listing timestamp is unknown for entities listed before its introduction
     */
    public static final long NO_LISTING_TIMESTAMP = 0;

    /**
     * The timestamp in milliseconds associated with the entity listed (see {@link ListableEntity#getTimestamp()}).
     */
    private final long timestamp;
    /**
     * The size in bytes of the entity listed (see {@link ListableEntity#getSize()}).
     */
    private final long size;
    /**
     * The most recent timestamp in milliseconds this entity was listed.
     */
    private final long listingTimestamp;

    public ListedEntity(long timestamp, long size) {
        this.timestamp = timestamp;
        this.size = size;
        this.listingTimestamp = NO_LISTING_TIMESTAMP;
    }

    @JsonCreator
    public ListedEntity(
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("size") long size,
            @JsonProperty("listingTimestamp") long listingTimestamp) {
        this.timestamp = timestamp;
        this.size = size;
        this.listingTimestamp = listingTimestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getSize() {
        return size;
    }

    public long getListingTimestamp() {
        return listingTimestamp;
    }
}
