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
package org.apache.nifi.provenance.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.search.SearchableField;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IndexingAction {
    private final Set<SearchableField> searchableEventFields;
    private final Set<SearchableField> searchableAttributeFields;

    public IndexingAction(final List<SearchableField> searchableEventFields, final List<SearchableField> searchableAttributes) {
        this.searchableEventFields = Collections.unmodifiableSet(new HashSet<>(searchableEventFields));
        this.searchableAttributeFields = Collections.unmodifiableSet(new HashSet<>(searchableAttributes));
    }

    private void addField(final Document doc, final SearchableField field, final String value, final Store store) {
        if (value == null || (!field.isAttribute() && !searchableEventFields.contains(field))) {
            return;
        }

        doc.add(new StringField(field.getSearchableFieldName(), value.toLowerCase(), store));
    }


    public void index(final StandardProvenanceEventRecord record, final IndexWriter indexWriter, final Integer blockIndex) throws IOException {
        final Document doc = new Document();
        addField(doc, SearchableFields.FlowFileUUID, record.getFlowFileUuid(), Store.NO);
        addField(doc, SearchableFields.Filename, record.getAttribute(CoreAttributes.FILENAME.key()), Store.NO);
        addField(doc, SearchableFields.ComponentID, record.getComponentId(), Store.NO);
        addField(doc, SearchableFields.AlternateIdentifierURI, record.getAlternateIdentifierUri(), Store.NO);
        addField(doc, SearchableFields.EventType, record.getEventType().name(), Store.NO);
        addField(doc, SearchableFields.Relationship, record.getRelationship(), Store.NO);
        addField(doc, SearchableFields.Details, record.getDetails(), Store.NO);
        addField(doc, SearchableFields.ContentClaimSection, record.getContentClaimSection(), Store.NO);
        addField(doc, SearchableFields.ContentClaimContainer, record.getContentClaimContainer(), Store.NO);
        addField(doc, SearchableFields.ContentClaimIdentifier, record.getContentClaimIdentifier(), Store.NO);
        addField(doc, SearchableFields.SourceQueueIdentifier, record.getSourceQueueIdentifier(), Store.NO);
        addField(doc, SearchableFields.TransitURI, record.getTransitUri(), Store.NO);

        for (final SearchableField searchableField : searchableAttributeFields) {
            addField(doc, searchableField, LuceneUtil.truncateIndexField(record.getAttribute(searchableField.getSearchableFieldName())), Store.NO);
        }

        final String storageFilename = LuceneUtil.substringBefore(record.getStorageFilename(), ".");

        // Index the fields that we always index (unless there's nothing else to index at all)
        if (!doc.getFields().isEmpty()) {
            doc.add(new LongPoint(SearchableFields.LineageStartDate.getSearchableFieldName(), record.getLineageStartDate()));
            doc.add(new LongPoint(SearchableFields.EventTime.getSearchableFieldName(), record.getEventTime()));
            doc.add(new LongPoint(SearchableFields.FileSize.getSearchableFieldName(), record.getFileSize()));
            doc.add(new StringField(FieldNames.STORAGE_FILENAME, storageFilename, Store.YES));

            if ( blockIndex == null ) {
                doc.add(new LongPoint(FieldNames.STORAGE_FILE_OFFSET, record.getStorageByteOffset()));
                doc.add(new StoredField(FieldNames.STORAGE_FILE_OFFSET, record.getStorageByteOffset()));
            } else {
                doc.add(new IntPoint(FieldNames.BLOCK_INDEX, blockIndex));
                doc.add(new StoredField(FieldNames.BLOCK_INDEX, blockIndex));

                doc.add(new LongPoint(SearchableFields.Identifier.getSearchableFieldName(), record.getEventId()));
                doc.add(new StoredField(SearchableFields.Identifier.getSearchableFieldName(), record.getEventId()));
            }

            // If it's event is a FORK, or JOIN, add the FlowFileUUID for all child/parent UUIDs.
            final ProvenanceEventType eventType = record.getEventType();
            if (eventType == ProvenanceEventType.FORK || eventType == ProvenanceEventType.CLONE || eventType == ProvenanceEventType.REPLAY) {
                for (final String uuid : record.getChildUuids()) {
                    if (!uuid.equals(record.getFlowFileUuid())) {
                        addField(doc, SearchableFields.FlowFileUUID, uuid, Store.NO);
                    }
                }
            } else if (eventType == ProvenanceEventType.JOIN) {
                for (final String uuid : record.getParentUuids()) {
                    if (!uuid.equals(record.getFlowFileUuid())) {
                        addField(doc, SearchableFields.FlowFileUUID, uuid, Store.NO);
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
                    addField(doc, SearchableFields.FlowFileUUID, sourceFlowFileUUID, Store.NO);
                }
            }

            indexWriter.addDocument(doc);
        }
    }
}
