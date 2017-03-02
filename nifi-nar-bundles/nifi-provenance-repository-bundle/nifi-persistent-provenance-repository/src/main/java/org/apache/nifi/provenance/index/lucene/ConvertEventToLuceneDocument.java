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
package org.apache.nifi.provenance.index.lucene;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.lucene.LuceneUtil;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.provenance.serialization.StorageSummary;

public class ConvertEventToLuceneDocument {
    private final Set<SearchableField> searchableEventFields;
    private final Set<SearchableField> searchableAttributeFields;

    public ConvertEventToLuceneDocument(final List<SearchableField> searchableEventFields, final List<SearchableField> searchableAttributes) {
        this.searchableEventFields = Collections.unmodifiableSet(new HashSet<>(searchableEventFields));
        this.searchableAttributeFields = Collections.unmodifiableSet(new HashSet<>(searchableAttributes));
    }

    private void addField(final Document doc, final SearchableField field, final String value) {
        if (value == null || (!field.isAttribute() && !searchableEventFields.contains(field))) {
            return;
        }

        doc.add(new StringField(field.getSearchableFieldName(), value.toLowerCase(), Store.NO));
    }


    public Document convert(final ProvenanceEventRecord record, final StorageSummary persistedEvent) {
        final Document doc = new Document();
        addField(doc, SearchableFields.FlowFileUUID, record.getFlowFileUuid());
        addField(doc, SearchableFields.Filename, record.getAttribute(CoreAttributes.FILENAME.key()));
        addField(doc, SearchableFields.ComponentID, record.getComponentId());
        addField(doc, SearchableFields.AlternateIdentifierURI, record.getAlternateIdentifierUri());
        addField(doc, SearchableFields.EventType, record.getEventType().name());
        addField(doc, SearchableFields.Relationship, record.getRelationship());
        addField(doc, SearchableFields.Details, record.getDetails());
        addField(doc, SearchableFields.ContentClaimSection, record.getContentClaimSection());
        addField(doc, SearchableFields.ContentClaimContainer, record.getContentClaimContainer());
        addField(doc, SearchableFields.ContentClaimIdentifier, record.getContentClaimIdentifier());
        addField(doc, SearchableFields.SourceQueueIdentifier, record.getSourceQueueIdentifier());
        addField(doc, SearchableFields.TransitURI, record.getTransitUri());

        for (final SearchableField searchableField : searchableAttributeFields) {
            addField(doc, searchableField, LuceneUtil.truncateIndexField(record.getAttribute(searchableField.getSearchableFieldName())));
        }

        // Index the fields that we always index (unless there's nothing else to index at all)
        if (!doc.getFields().isEmpty()) {
            // Always include Lineage Start Date because it allows us to make our Lineage queries more efficient.
            doc.add(new LongField(SearchableFields.LineageStartDate.getSearchableFieldName(), record.getLineageStartDate(), Store.NO));
            // Always include Event Time because most queries are bound by a start and end time.
            doc.add(new LongField(SearchableFields.EventTime.getSearchableFieldName(), record.getEventTime(), Store.NO));
            // We always include File Size because the UI wants to always render the controls for specifying this. This idea could be revisited.
            doc.add(new LongField(SearchableFields.FileSize.getSearchableFieldName(), record.getFileSize(), Store.NO));
            // We always store the event Event ID in the Document but do not index it. It doesn't make sense to query based on Event ID because
            // if we want a particular Event ID, we can just obtain it directly from the EventStore. But when we obtain a Document, this info must
            // be stored so that we know how to lookup the event in the store.
            doc.add(new UnIndexedLongField(SearchableFields.Identifier.getSearchableFieldName(), persistedEvent.getEventId()));

            // If it's event is a FORK, or JOIN, add the FlowFileUUID for all child/parent UUIDs.
            final ProvenanceEventType eventType = record.getEventType();
            if (eventType == ProvenanceEventType.FORK || eventType == ProvenanceEventType.CLONE || eventType == ProvenanceEventType.REPLAY) {
                for (final String uuid : record.getChildUuids()) {
                    if (!uuid.equals(record.getFlowFileUuid())) {
                        addField(doc, SearchableFields.FlowFileUUID, uuid);
                    }
                }
            } else if (eventType == ProvenanceEventType.JOIN) {
                for (final String uuid : record.getParentUuids()) {
                    if (!uuid.equals(record.getFlowFileUuid())) {
                        addField(doc, SearchableFields.FlowFileUUID, uuid);
                    }
                }
            } else if (eventType == ProvenanceEventType.RECEIVE && record.getSourceSystemFlowFileIdentifier() != null) {
                // If we get a receive with a Source System FlowFile Identifier, we add another Document that shows the UUID
                // that the Source System uses to refer to the data.
                final String sourceIdentifier = record.getSourceSystemFlowFileIdentifier();
                final String sourceFlowFileUUID;
                final int lastColon = sourceIdentifier.lastIndexOf(":");
                if (lastColon > -1 && lastColon < sourceIdentifier.length() - 2) {
                    sourceFlowFileUUID = sourceIdentifier.substring(lastColon + 1);
                } else {
                    sourceFlowFileUUID = null;
                }

                if (sourceFlowFileUUID != null) {
                    addField(doc, SearchableFields.FlowFileUUID, sourceFlowFileUUID);
                }
            }

            return doc;
        }

        return null;
    }

    private static class UnIndexedLongField extends Field {
        static final FieldType TYPE = new FieldType();
        static {
            TYPE.setIndexed(false);
            TYPE.setTokenized(true);
            TYPE.setOmitNorms(true);
            TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
            TYPE.setNumericType(FieldType.NumericType.LONG);
            TYPE.setStored(true);
            TYPE.freeze();
        }

        public UnIndexedLongField(String name, long value) {
            super(name, TYPE);
            fieldsData = Long.valueOf(value);
        }
    }
}
