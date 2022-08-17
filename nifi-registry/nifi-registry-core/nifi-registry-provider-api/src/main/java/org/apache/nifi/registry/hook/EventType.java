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

package org.apache.nifi.registry.hook;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Enumeration of possible EventTypes with the expected fields for each event.
 *
 * Producers of events must produce events with the fields in the same order specified here.
 */
public enum EventType {

    CREATE_BUCKET(
            EventFieldName.BUCKET_ID,
            EventFieldName.USER),
    CREATE_FLOW(
            EventFieldName.BUCKET_ID,
            EventFieldName.FLOW_ID,
            EventFieldName.USER),
    CREATE_FLOW_VERSION(
            EventFieldName.BUCKET_ID,
            EventFieldName.FLOW_ID,
            EventFieldName.VERSION,
            EventFieldName.USER,
            EventFieldName.COMMENT),
    CREATE_EXTENSION_BUNDLE(
            EventFieldName.BUCKET_ID,
            EventFieldName.EXTENSION_BUNDLE_ID,
            EventFieldName.USER
    ),
    CREATE_EXTENSION_BUNDLE_VERSION(
            EventFieldName.BUCKET_ID,
            EventFieldName.EXTENSION_BUNDLE_ID,
            EventFieldName.VERSION,
            EventFieldName.USER
    ),
    REGISTRY_START(),
    UPDATE_BUCKET(
            EventFieldName.BUCKET_ID,
            EventFieldName.USER),
    UPDATE_FLOW(
            EventFieldName.BUCKET_ID,
            EventFieldName.FLOW_ID,
            EventFieldName.USER),
    DELETE_BUCKET(
            EventFieldName.BUCKET_ID,
            EventFieldName.USER),
    DELETE_FLOW(
            EventFieldName.BUCKET_ID,
            EventFieldName.FLOW_ID,
            EventFieldName.USER),
    DELETE_EXTENSION_BUNDLE(
            EventFieldName.BUCKET_ID,
            EventFieldName.EXTENSION_BUNDLE_ID,
            EventFieldName.USER
    ),
    DELETE_EXTENSION_BUNDLE_VERSION(
            EventFieldName.BUCKET_ID,
            EventFieldName.EXTENSION_BUNDLE_ID,
            EventFieldName.VERSION,
            EventFieldName.USER
    ),
    CREATE_USER(
            EventFieldName.USER_ID,
            EventFieldName.USER_IDENTITY
    ),
    UPDATE_USER(
            EventFieldName.USER_ID,
            EventFieldName.USER_IDENTITY
    ),
    DELETE_USER(
            EventFieldName.USER_ID,
            EventFieldName.USER_IDENTITY
    ),
    CREATE_USER_GROUP(
            EventFieldName.USER_GROUP_ID,
            EventFieldName.USER_GROUP_IDENTITY
    ),
    UPDATE_USER_GROUP(
            EventFieldName.USER_GROUP_ID,
            EventFieldName.USER_GROUP_IDENTITY
    ),
    DELETE_USER_GROUP(
            EventFieldName.USER_GROUP_ID,
            EventFieldName.USER_GROUP_IDENTITY
    )
    ;


    private List<EventFieldName> fieldNames;

    EventType(EventFieldName... fieldNames) {
        this.fieldNames = Collections.unmodifiableList(Arrays.asList(fieldNames));
    }

    public List<EventFieldName> getFieldNames() {
        return this.fieldNames;
    }

}
