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
package org.apache.nifi.services.ethereum;

import java.util.Objects;

/**
 * Minimal Beacon Event representation decoupled from any external library.
 * Data is represented as a raw JSON string to avoid API dependencies.
 */
public final class BeaconEvent {
    private final EventType eventType;
    private final String jsonData;

    public BeaconEvent(final EventType eventType, final String jsonData) {
        this.eventType = Objects.requireNonNull(eventType, "eventType");
        this.jsonData = Objects.requireNonNull(jsonData, "jsonData");
    }

    public EventType getEventType() {
        return eventType;
    }

    /**
     * Returns the event payload as a JSON string.
     */
    public String getJsonData() {
        return jsonData;
    }

    @Override
    public String toString() {
        return "BeaconEvent{" +
                "eventType=" + eventType +
                ", jsonData='" + jsonData + '\'' +
                '}';
    }
}
