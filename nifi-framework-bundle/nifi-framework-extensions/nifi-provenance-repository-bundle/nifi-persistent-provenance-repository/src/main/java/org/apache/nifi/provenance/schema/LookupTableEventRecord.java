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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.repository.schema.FieldMapRecord;
import org.apache.nifi.repository.schema.NamedValue;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;

public class LookupTableEventRecord implements Record {
    private final RecordSchema schema;
    private final ProvenanceEventRecord event;
    private final long eventId;
    private final Record contentClaimRecord;
    private final Record previousClaimRecord;

    private final long eventIdStartOffset;
    private final long startTimeOffset;
    private final Map<String, Integer> componentIdMap;
    private final Map<String, Integer> componentTypeMap;
    private final Map<String, Integer> queueIdMap;
    private final Map<String, Integer> eventTypeMap;

    public LookupTableEventRecord(final ProvenanceEventRecord event, final long eventId, final RecordSchema schema, final RecordSchema contentClaimSchema,
        final RecordSchema previousContentClaimSchema, final long eventIdStartOffset, final long startTimeOffset, final Map<String, Integer> componentIdMap,
        final Map<String, Integer> componentTypeMap, final Map<String, Integer> queueIdMap, final Map<String, Integer> eventTypeMap) {
        this.schema = schema;
        this.event = event;
        this.eventId = eventId;
        this.previousClaimRecord = createPreviousContentClaimRecord(previousContentClaimSchema, event.getPreviousContentClaimContainer(), event.getPreviousContentClaimSection(),
            event.getPreviousContentClaimIdentifier(), event.getPreviousContentClaimOffset(), event.getPreviousFileSize());
        this.contentClaimRecord = createContentClaimRecord(contentClaimSchema, event.getContentClaimContainer(), event.getContentClaimSection(),
            event.getContentClaimIdentifier(), event.getContentClaimOffset(), event.getFileSize());

        this.eventIdStartOffset = eventIdStartOffset;
        this.startTimeOffset = startTimeOffset;
        this.componentIdMap = componentIdMap;
        this.componentTypeMap = componentTypeMap;
        this.queueIdMap = queueIdMap;
        this.eventTypeMap = eventTypeMap;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }


    private static Record createPreviousContentClaimRecord(final RecordSchema contentClaimSchema, final String container, final String section,
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

    private static Record createContentClaimRecord(final RecordSchema contentClaimSchema, final String container, final String section,
            final String identifier, final Long offset, final Long size) {

        if (container == null || section == null || identifier == null) {
            final Map<RecordField, Object> lookupValues = Collections.singletonMap(LookupTableEventRecordFields.NO_VALUE, EventFieldNames.NO_VALUE);
            final List<RecordField> noValueFields = Collections.singletonList(contentClaimSchema.getField(EventFieldNames.NO_VALUE));
            return new FieldMapRecord(lookupValues, new RecordSchema(noValueFields));
        }

        final Map<RecordField, Object> fieldValues = new HashMap<>();
        fieldValues.put(EventRecordFields.CONTENT_CLAIM_CONTAINER, container);
        fieldValues.put(EventRecordFields.CONTENT_CLAIM_SECTION, section);
        fieldValues.put(EventRecordFields.CONTENT_CLAIM_IDENTIFIER, identifier);
        fieldValues.put(EventRecordFields.CONTENT_CLAIM_OFFSET, offset);
        fieldValues.put(EventRecordFields.CONTENT_CLAIM_SIZE, size);

        final List<RecordField> explicitClaimFields = contentClaimSchema.getField(EventFieldNames.EXPLICIT_VALUE).getSubFields();
        final Record explicitClaimRecord = new FieldMapRecord(fieldValues, new RecordSchema(explicitClaimFields));
        return explicitClaimRecord;
    }

    private static String readLookupValue(final Object recordValue, final List<String> lookup) {
        if (recordValue == null) {
            return null;
        }

        // NO_VALUE type
        if (recordValue instanceof Boolean) {
            return null;
        }

        // LOOKUP type
        if (recordValue instanceof Integer) {
            final Integer indexValue = (Integer) recordValue;
            final int index = indexValue.intValue();
            if (index > lookup.size() - 1) {
                return null;
            }

            return lookup.get(index);
        }

        // EXPLICIT_VALUE type
        if (recordValue instanceof String) {
            return (String) recordValue;
        }

        return null;
    }

    private NamedValue createLookupValue(final String literalValue, final Map<String, Integer> lookup) {
        if (literalValue == null) {
            final Map<RecordField, Object> lookupValues = Collections.singletonMap(LookupTableEventRecordFields.NO_VALUE, EventFieldNames.NO_VALUE);
            final Record record = new FieldMapRecord(lookupValues, LookupTableEventSchema.NO_VALUE_SCHEMA);
            final NamedValue namedValue = new NamedValue(EventFieldNames.NO_VALUE, record);
            return namedValue;
        }

        final Integer index = lookup.get(literalValue);
        if (index == null) {
            final Map<RecordField, Object> lookupValues = Collections.singletonMap(LookupTableEventRecordFields.EXPLICIT_STRING, literalValue);
            final Record record = new FieldMapRecord(lookupValues, LookupTableEventSchema.EXPLICIT_STRING_SCHEMA);
            final NamedValue namedValue = new NamedValue(EventFieldNames.EXPLICIT_VALUE, record);
            return namedValue;
        } else {
            final Map<RecordField, Object> lookupValues = Collections.singletonMap(LookupTableEventRecordFields.LOOKUP_VALUE, index);
            final Record record = new FieldMapRecord(lookupValues, LookupTableEventSchema.LOOKUP_VALUE_SCHEMA);
            final NamedValue namedValue = new NamedValue(EventFieldNames.LOOKUP_VALUE, record);
            return namedValue;
        }
    }

    private NamedValue createExplicitSameOrNoneValue(final Record newValue, final Record oldValue, final Supplier<Record> recordSupplier) {
        if (newValue == null || EventFieldNames.NO_VALUE.equals(newValue.getSchema().getFields().get(0).getFieldName())) {
            final Map<RecordField, Object> lookupValues = Collections.singletonMap(LookupTableEventRecordFields.NO_VALUE, EventFieldNames.NO_VALUE);
            final Record record = new FieldMapRecord(lookupValues, LookupTableEventSchema.NO_VALUE_SCHEMA);
            final NamedValue namedValue = new NamedValue(EventFieldNames.NO_VALUE, record);
            return namedValue;
        } else if (newValue.equals(oldValue)) {
            final Map<RecordField, Object> lookupValues = Collections.singletonMap(LookupTableEventRecordFields.UNCHANGED_VALUE, EventFieldNames.UNCHANGED_VALUE);
            final Record record = new FieldMapRecord(lookupValues, LookupTableEventSchema.UNCHANGED_VALUE_SCHEMA);
            final NamedValue namedValue = new NamedValue(EventFieldNames.UNCHANGED_VALUE, record);
            return namedValue;
        }

        final Record record = recordSupplier.get();
        final NamedValue namedValue = new NamedValue(EventFieldNames.EXPLICIT_VALUE, record);
        return namedValue;
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        switch (fieldName) {
            case EventFieldNames.EVENT_IDENTIFIER:
                return (int) (eventId - eventIdStartOffset);
            case EventFieldNames.ALTERNATE_IDENTIFIER:
                return event.getAlternateIdentifierUri();
            case EventFieldNames.CHILD_UUIDS:
                return event.getChildUuids();
            case EventFieldNames.COMPONENT_ID:
                return createLookupValue(event.getComponentId(), componentIdMap);
            case EventFieldNames.COMPONENT_TYPE:
                return createLookupValue(event.getComponentType(), componentTypeMap);
            case EventFieldNames.CONTENT_CLAIM:
                return createExplicitSameOrNoneValue(contentClaimRecord, previousClaimRecord, () -> contentClaimRecord);
            case EventFieldNames.EVENT_DETAILS:
                return event.getDetails();
            case EventFieldNames.EVENT_DURATION:
                return (int) event.getEventDuration();
            case EventFieldNames.EVENT_TIME:
                return (int) (event.getEventTime() - startTimeOffset);
            case EventFieldNames.EVENT_TYPE:
                return eventTypeMap.get(event.getEventType().name());
            case EventFieldNames.FLOWFILE_ENTRY_DATE:
                return (int) (event.getFlowFileEntryDate() - startTimeOffset);
            case EventFieldNames.LINEAGE_START_DATE:
                return (int) (event.getLineageStartDate() - startTimeOffset);
            case EventFieldNames.PARENT_UUIDS:
                return event.getParentUuids();
            case EventFieldNames.PREVIOUS_ATTRIBUTES:
                return event.getPreviousAttributes();
            case EventFieldNames.PREVIOUS_CONTENT_CLAIM:
                return previousClaimRecord;
            case EventFieldNames.RELATIONSHIP:
                return event.getRelationship();
            case EventFieldNames.SOURCE_QUEUE_IDENTIFIER:
                return createLookupValue(event.getSourceQueueIdentifier(), queueIdMap);
            case EventFieldNames.SOURCE_SYSTEM_FLOWFILE_IDENTIFIER:
                return event.getSourceSystemFlowFileIdentifier();
            case EventFieldNames.TRANSIT_URI:
                return event.getTransitUri();
            case EventFieldNames.UPDATED_ATTRIBUTES:
                return event.getUpdatedAttributes();
            case EventFieldNames.FLOWFILE_UUID:
                return event.getAttribute(CoreAttributes.UUID.key());
        }

        return null;
    }

    private static Long addLong(final Integer optionalValue, final long requiredValue) {
        if (optionalValue == null) {
            return null;
        }

        return optionalValue.longValue() + requiredValue;
    }

    @SuppressWarnings("unchecked")
    public static StandardProvenanceEventRecord getEvent(final Record record, final String storageFilename, final long storageByteOffset, final int maxAttributeLength,
        final long eventIdStartOffset, final long startTimeOffset, final List<String> componentIds, final List<String> componentTypes,
        final List<String> queueIds, final List<String> eventTypes) {

        final Map<String, String> previousAttributes = truncateAttributes((Map<String, String>) record.getFieldValue(EventFieldNames.PREVIOUS_ATTRIBUTES), maxAttributeLength);
        final Map<String, String> updatedAttributes = truncateAttributes((Map<String, String>) record.getFieldValue(EventFieldNames.UPDATED_ATTRIBUTES), maxAttributeLength);

        final List<String> childUuids = (List<String>) record.getFieldValue(EventFieldNames.CHILD_UUIDS);
        final List<String> parentUuids = (List<String>) record.getFieldValue(EventFieldNames.PARENT_UUIDS);

        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();
        builder.setAlternateIdentifierUri((String) record.getFieldValue(EventFieldNames.ALTERNATE_IDENTIFIER));
        builder.setChildUuids(childUuids);
        builder.setDetails((String) record.getFieldValue(EventFieldNames.EVENT_DETAILS));
        builder.setParentUuids(parentUuids);
        builder.setPreviousAttributes(previousAttributes);
        builder.setRelationship((String) record.getFieldValue(EventFieldNames.RELATIONSHIP));
        builder.setSourceSystemFlowFileIdentifier((String) record.getFieldValue(EventFieldNames.SOURCE_SYSTEM_FLOWFILE_IDENTIFIER));
        builder.setTransitUri((String) record.getFieldValue(EventFieldNames.TRANSIT_URI));
        builder.setUpdatedAttributes(updatedAttributes);


        builder.setComponentId(readLookupValue(record.getFieldValue(EventFieldNames.COMPONENT_ID), componentIds));
        builder.setComponentType(readLookupValue(record.getFieldValue(EventFieldNames.COMPONENT_TYPE), componentTypes));
        builder.setSourceQueueIdentifier(readLookupValue(record.getFieldValue(EventFieldNames.SOURCE_QUEUE_IDENTIFIER), queueIds));

        // Determine the event type
        final Integer eventTypeOrdinal = (Integer) record.getFieldValue(EventFieldNames.EVENT_TYPE);
        ProvenanceEventType eventType;
        if (eventTypeOrdinal == null || eventTypeOrdinal > eventTypes.size() || eventTypeOrdinal < 0) {
            eventType = ProvenanceEventType.UNKNOWN;
        } else {
            try {
                eventType = ProvenanceEventType.valueOf(eventTypes.get(eventTypeOrdinal));
            } catch (final Exception e) {
                eventType = ProvenanceEventType.UNKNOWN;
            }
        }
        builder.setEventType(eventType);

        // Determine appropriate UUID for the event
        String uuid = null;
        switch (eventType) {
            case CLONE:
            case FORK:
            case REPLAY:
                if (parentUuids != null && !parentUuids.isEmpty()) {
                    uuid = parentUuids.get(0);
                }
                break;
            case JOIN:
                if (childUuids != null && !childUuids.isEmpty()) {
                    uuid = childUuids.get(0);
                }
                break;
        }

        if (uuid == null) {
            uuid = updatedAttributes == null ? null : updatedAttributes.get(CoreAttributes.UUID.key());
            if (uuid == null) {
                uuid = previousAttributes == null ? null : previousAttributes.get(CoreAttributes.UUID.key());
            }
        }

        builder.setFlowFileUUID(uuid);

        builder.setEventDuration((Integer) record.getFieldValue(EventFieldNames.EVENT_DURATION));
        builder.setEventTime(addLong((Integer) record.getFieldValue(EventFieldNames.EVENT_TIME), startTimeOffset));
        builder.setFlowFileEntryDate(addLong((Integer) record.getFieldValue(EventFieldNames.FLOWFILE_ENTRY_DATE), startTimeOffset));
        builder.setLineageStartDate(addLong((Integer) record.getFieldValue(EventFieldNames.LINEAGE_START_DATE), startTimeOffset));

        final Integer eventId = (Integer) record.getFieldValue(EventFieldNames.EVENT_IDENTIFIER);
        if (eventId != null) {
            builder.setEventId(eventId.longValue() + eventIdStartOffset);
        }

        builder.setStorageLocation(storageFilename, storageByteOffset);

        final Record previousClaimRecord = (Record) record.getFieldValue(EventFieldNames.PREVIOUS_CONTENT_CLAIM);
        if (previousClaimRecord != null) {
            builder.setPreviousContentClaim(
                (String) previousClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_CONTAINER),
                (String) previousClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_SECTION),
                (String) previousClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_IDENTIFIER),
                (Long) previousClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_OFFSET),
                (Long) previousClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_SIZE));
        }

        final Object contentClaimObject = record.getFieldValue(EventFieldNames.CONTENT_CLAIM);

        // NO_VALUE type
        builder.setCurrentContentClaim(null, null, null, null, 0L);
        if (contentClaimObject != null) {
            if (contentClaimObject instanceof String) {
                final String contentClaimDescription = (String) contentClaimObject;
                switch (contentClaimDescription) {
                    case EventFieldNames.UNCHANGED_VALUE:
                        builder.setCurrentContentClaim((String) previousClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_CONTAINER),
                            (String) previousClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_SECTION),
                            (String) previousClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_IDENTIFIER),
                            (Long) previousClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_OFFSET),
                            (Long) previousClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_SIZE));
                        break;
                }
            } else if (contentClaimObject instanceof Record) {
                final Record currentClaimRecord = (Record) contentClaimObject;
                builder.setCurrentContentClaim(
                    (String) currentClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_CONTAINER),
                    (String) currentClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_SECTION),
                    (String) currentClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_IDENTIFIER),
                    (Long) currentClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_OFFSET),
                    (Long) currentClaimRecord.getFieldValue(EventFieldNames.CONTENT_CLAIM_SIZE));
            }
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
