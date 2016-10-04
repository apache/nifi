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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.repository.schema.FieldMapRecord;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;

public class EventRecord implements Record {
    private final RecordSchema schema;
    private final ProvenanceEventRecord event;
    private final long eventId;
    private final Record contentClaimRecord;
    private final Record previousClaimRecord;

    public EventRecord(final ProvenanceEventRecord event, final long eventId, final RecordSchema schema, final RecordSchema contentClaimSchema) {
        this.schema = schema;
        this.event = event;
        this.eventId = eventId;
        this.contentClaimRecord = createContentClaimRecord(contentClaimSchema, event.getContentClaimContainer(), event.getContentClaimSection(),
            event.getContentClaimIdentifier(), event.getContentClaimOffset(), event.getFileSize());
        this.previousClaimRecord = createContentClaimRecord(contentClaimSchema, event.getPreviousContentClaimContainer(), event.getPreviousContentClaimSection(),
            event.getPreviousContentClaimIdentifier(), event.getPreviousContentClaimOffset(), event.getPreviousFileSize());
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    private static Record createContentClaimRecord(final RecordSchema contentClaimSchema, final String container, final String section,
            final String identifier, final Long offset, final Long size) {
        if (container == null || section == null || identifier == null) {
            return null;
        }

        final Map<RecordField, Object> fieldValues = new HashMap<>();
        fieldValues.put(EventRecordFields.CONTENT_CLAIM_CONTAINER, container);
        fieldValues.put(EventRecordFields.CONTENT_CLAIM_SECTION, section);
        fieldValues.put(EventRecordFields.CONTENT_CLAIM_IDENTIFIER, identifier);
        fieldValues.put(EventRecordFields.CONTENT_CLAIM_OFFSET, offset);
        fieldValues.put(EventRecordFields.CONTENT_CLAIM_SIZE, size);
        return new FieldMapRecord(fieldValues, contentClaimSchema);
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        switch (fieldName) {
            case EventRecordFields.Names.EVENT_IDENTIFIER:
                return eventId;
            case EventRecordFields.Names.ALTERNATE_IDENTIFIER:
                return event.getAlternateIdentifierUri();
            case EventRecordFields.Names.CHILD_UUIDS:
                return event.getChildUuids();
            case EventRecordFields.Names.COMPONENT_ID:
                return event.getComponentId();
            case EventRecordFields.Names.COMPONENT_TYPE:
                return event.getComponentType();
            case EventRecordFields.Names.CONTENT_CLAIM:
                return contentClaimRecord;
            case EventRecordFields.Names.EVENT_DETAILS:
                return event.getDetails();
            case EventRecordFields.Names.EVENT_DURATION:
                return event.getEventDuration();
            case EventRecordFields.Names.EVENT_TIME:
                return event.getEventTime();
            case EventRecordFields.Names.EVENT_TYPE:
                return event.getEventType().name();
            case EventRecordFields.Names.FLOWFILE_ENTRY_DATE:
                return event.getFlowFileEntryDate();
            case EventRecordFields.Names.FLOWFILE_UUID:
                return event.getFlowFileUuid();
            case EventRecordFields.Names.LINEAGE_START_DATE:
                return event.getLineageStartDate();
            case EventRecordFields.Names.PARENT_UUIDS:
                return event.getParentUuids();
            case EventRecordFields.Names.PREVIOUS_ATTRIBUTES:
                return event.getPreviousAttributes();
            case EventRecordFields.Names.PREVIOUS_CONTENT_CLAIM:
                return previousClaimRecord;
            case EventRecordFields.Names.RELATIONSHIP:
                return event.getRelationship();
            case EventRecordFields.Names.SOURCE_QUEUE_IDENTIFIER:
                return event.getSourceQueueIdentifier();
            case EventRecordFields.Names.SOURCE_SYSTEM_FLOWFILE_IDENTIFIER:
                return event.getSourceSystemFlowFileIdentifier();
            case EventRecordFields.Names.TRANSIT_URI:
                return event.getTransitUri();
            case EventRecordFields.Names.UPDATED_ATTRIBUTES:
                return event.getUpdatedAttributes();
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    public static StandardProvenanceEventRecord getEvent(final Record record, final String storageFilename, final long storageByteOffset, final int maxAttributeLength) {
        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();
        builder.setAlternateIdentifierUri((String) record.getFieldValue(EventRecordFields.Names.ALTERNATE_IDENTIFIER));
        builder.setChildUuids((List<String>) record.getFieldValue(EventRecordFields.Names.CHILD_UUIDS));
        builder.setComponentId((String) record.getFieldValue(EventRecordFields.Names.COMPONENT_ID));
        builder.setComponentType((String) record.getFieldValue(EventRecordFields.Names.COMPONENT_TYPE));
        builder.setDetails((String) record.getFieldValue(EventRecordFields.Names.EVENT_DETAILS));
        builder.setEventDuration((Long) record.getFieldValue(EventRecordFields.Names.EVENT_DURATION));
        builder.setEventTime((Long) record.getFieldValue(EventRecordFields.Names.EVENT_TIME));
        builder.setEventType(ProvenanceEventType.valueOf((String) record.getFieldValue(EventRecordFields.Names.EVENT_TYPE)));
        builder.setFlowFileEntryDate((Long) record.getFieldValue(EventRecordFields.Names.FLOWFILE_ENTRY_DATE));
        builder.setFlowFileUUID((String) record.getFieldValue(EventRecordFields.Names.FLOWFILE_UUID));
        builder.setLineageStartDate((Long) record.getFieldValue(EventRecordFields.Names.LINEAGE_START_DATE));
        builder.setParentUuids((List<String>) record.getFieldValue(EventRecordFields.Names.PARENT_UUIDS));
        builder.setPreviousAttributes(truncateAttributes((Map<String, String>) record.getFieldValue(EventRecordFields.Names.PREVIOUS_ATTRIBUTES), maxAttributeLength));
        builder.setEventId((Long) record.getFieldValue(EventRecordFields.Names.EVENT_IDENTIFIER));
        builder.setRelationship((String) record.getFieldValue(EventRecordFields.Names.RELATIONSHIP));
        builder.setSourceQueueIdentifier((String) record.getFieldValue(EventRecordFields.Names.SOURCE_QUEUE_IDENTIFIER));
        builder.setSourceSystemFlowFileIdentifier((String) record.getFieldValue(EventRecordFields.Names.SOURCE_SYSTEM_FLOWFILE_IDENTIFIER));
        builder.setTransitUri((String) record.getFieldValue(EventRecordFields.Names.TRANSIT_URI));
        builder.setUpdatedAttributes(truncateAttributes((Map<String, String>) record.getFieldValue(EventRecordFields.Names.UPDATED_ATTRIBUTES), maxAttributeLength));

        builder.setStorageLocation(storageFilename, storageByteOffset);

        final Record currentClaimRecord = (Record) record.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM);
        if (currentClaimRecord == null) {
            builder.setCurrentContentClaim(null, null, null, null, 0L);
        } else {
            builder.setCurrentContentClaim(
                (String) currentClaimRecord.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM_CONTAINER),
                (String) currentClaimRecord.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM_SECTION),
                (String) currentClaimRecord.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM_IDENTIFIER),
                (Long) currentClaimRecord.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM_OFFSET),
                (Long) currentClaimRecord.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM_SIZE));
        }

        final Record previousClaimRecord = (Record) record.getFieldValue(EventRecordFields.Names.PREVIOUS_CONTENT_CLAIM);
        if (previousClaimRecord != null) {
            builder.setPreviousContentClaim(
                (String) previousClaimRecord.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM_CONTAINER),
                (String) previousClaimRecord.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM_SECTION),
                (String) previousClaimRecord.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM_IDENTIFIER),
                (Long) previousClaimRecord.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM_OFFSET),
                (Long) previousClaimRecord.getFieldValue(EventRecordFields.Names.CONTENT_CLAIM_SIZE));
        }

        return builder.build();
    }

    private static Map<String, String> truncateAttributes(final Map<String, String> attributes, final int maxAttributeLength) {
        if (attributes == null) {
            return null;
        }

        // Check if any attribute value exceeds the attribute length
        final boolean anyExceedsLength = attributes.values().stream()
            .filter(value -> value != null)
            .anyMatch(value -> value.length() > maxAttributeLength);

        if (!anyExceedsLength) {
            return attributes;
        }

        final Map<String, String> truncated = new HashMap<>();
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();

            if (value == null || value.length() <= maxAttributeLength) {
                truncated.put(key, value);
                continue;
            }

            truncated.put(key, value.substring(0, maxAttributeLength));
        }

        return truncated;
    }
}
