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

package org.apache.nifi.provenance.schema;

public class EventFieldNames {
    public static final String EVENT_IDENTIFIER = "Event ID";
    public static final String EVENT_TYPE = "Event Type";
    public static final String EVENT_TIME = "Event Time";
    public static final String FLOWFILE_ENTRY_DATE = "FlowFile Entry Date";
    public static final String EVENT_DURATION = "Event Duration";
    public static final String LINEAGE_START_DATE = "Lineage Start Date";
    public static final String COMPONENT_ID = "Component ID";
    public static final String COMPONENT_TYPE = "Component Type";
    public static final String FLOWFILE_UUID = "FlowFile UUID";
    public static final String EVENT_DETAILS = "Event Details";
    public static final String SOURCE_QUEUE_IDENTIFIER = "Source Queue Identifier";
    public static final String CONTENT_CLAIM = "Content Claim";
    public static final String PREVIOUS_CONTENT_CLAIM = "Previous Content Claim";
    public static final String EXPLICIT_CURRENT_CONTENT_CLAIM = "Full Current Content Claim";
    public static final String PARENT_UUIDS = "Parent UUIDs";
    public static final String CHILD_UUIDS = "Child UUIDs";

    public static final String ATTRIBUTE_NAME = "Attribute Name";
    public static final String ATTRIBUTE_VALUE = "Attribute Value";
    public static final String PREVIOUS_ATTRIBUTES = "Previous Attributes";
    public static final String UPDATED_ATTRIBUTES = "Updated Attributes";

    public static final String CONTENT_CLAIM_CONTAINER = "Content Claim Container";
    public static final String CONTENT_CLAIM_SECTION = "Content Claim Section";
    public static final String CONTENT_CLAIM_IDENTIFIER = "Content Claim Identifier";
    public static final String CONTENT_CLAIM_OFFSET = "Content Claim Offset";
    public static final String CONTENT_CLAIM_SIZE = "Content Claim Size";

    public static final String TRANSIT_URI = "Transit URI";
    public static final String SOURCE_SYSTEM_FLOWFILE_IDENTIFIER = "Source System FlowFile Identifier";
    public static final String ALTERNATE_IDENTIFIER = "Alternate Identifier";
    public static final String RELATIONSHIP = "Relationship";

    // For Lookup Tables
    public static final String NO_VALUE = "No Value";
    public static final String EXPLICIT_VALUE = "Explicit Value";
    public static final String LOOKUP_VALUE = "Lookup Value";
    public static final String UNCHANGED_VALUE = "Unchanged";

    // For encrypted records
    public static final String IS_ENCRYPTED = "Encrypted Record";
    public static final String KEY_ID = "Encryption Key ID";
    public static final String VERSION = "Encryption Version";
    public static final String ALGORITHM = "Encryption Algorithm";
    public static final String IV = "Initialization Vector";
    public static final String ENCRYPTION_DETAILS = "Encryption Details";
}
