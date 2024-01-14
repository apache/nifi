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

import org.apache.nifi.components.DescribedValue;

public enum EntityTrackingMode implements DescribedValue {
    TRACK_ENTITY_TIMESTAMP(
            "Track Entity Timestamp",
            "Use the timestamp of the listed entity for tracking. " +
                    "This mode can pick any entity whose timestamp is inside the specified time window. " +
                    "For example, if the 'Entity Tracking Time Window' is set to '30 minutes', " +
                    "any entity having timestamp in recent 30 minutes will be the listing target when this processor runs. " +
                    "All cache entries leaving the time window, being older than 30 minutes, will be removed from the cache. " +
                    "In return, any entity having a timestamp older than 30 minutes will be ignored from listing, " +
                    "as its cache entry would be removed from the cache immediately and thus listed over and over again. " +
                    "There can be an exception to this behaviour on the initial listing, see 'Entity Tracking Initial Listing Target'."
    ),
    TRACK_LAST_LISTING_TIME(
            "Track Last Listing Time",
            "Use last time the entity was listed for tracking. " +
                    "This mode can pick any entity regardless of its timestamp. " +
                    "For example, if the 'Entity Tracking Time Window' is set to '30 minutes', " +
                    "all entities will be listing target when this processor runs initially " +
                    "and their cache entry will be cached for at least 30 minutes. " +
                    "In case the entity is listed again, its cache entry is updated its 30 minutes of expiration start over. " +
                    "After 30 minutes of the entity not having been listed, e.g. due to it being deleted from the source, " +
                    "its cache entry expires and is removed."
    );

    private final String value;
    private final String description;

    EntityTrackingMode(String value, String description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return value;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
