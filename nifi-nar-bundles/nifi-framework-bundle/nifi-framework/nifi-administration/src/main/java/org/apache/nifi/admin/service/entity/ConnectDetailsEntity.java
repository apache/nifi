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
package org.apache.nifi.admin.service.entity;

/**
 * Enumeration of Action Connect Details properties stored as Entity objects
 */
public enum ConnectDetailsEntity implements EntityProperty {
    ACTION("action"),

    SOURCE_ID("sourceId"),

    SOURCE_NAME("sourceName"),

    SOURCE_TYPE("sourceType"),

    DESTINATION_ID("destinationId"),

    DESTINATION_NAME("destinationName"),

    DESTINATION_TYPE("destinationType"),

    RELATIONSHIP("relationship");

    private final String property;

    ConnectDetailsEntity(final String property) {
        this.property = property;
    }

    @Override
    public String getProperty() {
        return property;
    }
}
