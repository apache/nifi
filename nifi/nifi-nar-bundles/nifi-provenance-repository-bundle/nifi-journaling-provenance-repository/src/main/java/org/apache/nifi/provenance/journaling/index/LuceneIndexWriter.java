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
package org.apache.nifi.provenance.journaling.index;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.journaling.JournaledProvenanceEvent;
import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.search.SearchableField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneIndexWriter implements EventIndexWriter {
    private static final Logger logger = LoggerFactory.getLogger(LuceneIndexWriter.class);
    
    @SuppressWarnings("unused")
    private final JournalingRepositoryConfig config;
    private final Set<SearchableField> nonAttributeSearchableFields;
    private final Set<SearchableField> attributeSearchableFields;
    
    private final Directory directory;
    private final Analyzer analyzer;
    private final IndexWriter indexWriter;
    private final AtomicLong indexMaxId = new AtomicLong(-1L);
    
    public LuceneIndexWriter(final File indexDir, final JournalingRepositoryConfig config) throws IOException {
        this.config = config;
        
        attributeSearchableFields = Collections.unmodifiableSet(new HashSet<>(config.getSearchableAttributes()));
        nonAttributeSearchableFields = Collections.unmodifiableSet(new HashSet<>(config.getSearchableFields()));
        
        directory = FSDirectory.open(indexDir);
        analyzer = new StandardAnalyzer();
        final IndexWriterConfig writerConfig = new IndexWriterConfig(Version.LATEST, analyzer);
        indexWriter = new IndexWriter(directory, writerConfig);
    }
    
    public EventIndexSearcher newIndexSearcher() throws IOException {
        final DirectoryReader reader = DirectoryReader.open(indexWriter, false);
        return new LuceneIndexSearcher(reader);
    }

    @Override
    public void close() throws IOException {
        IOException suppressed = null;
        try {
            indexWriter.close();
        } catch (final IOException ioe) {
            suppressed = ioe;
        }
        
        analyzer.close();
        
        try {
            directory.close();
        } catch (final IOException ioe) {
            if ( suppressed != null ) {
                ioe.addSuppressed(suppressed);
            }
            
            throw ioe;
        }
    }
    
    
    private void addField(final Document doc, final SearchableField field, final String value, final Store store) {
        if (value == null || (!nonAttributeSearchableFields.contains(field) && !field.isAttribute())) {
            return;
        }

        doc.add(new StringField(field.getSearchableFieldName(), value.toLowerCase(), store));
    }
    
    @Override
    public void index(final Collection<JournaledProvenanceEvent> events) throws IOException {
        long maxId = this.indexMaxId.get();
        
        for ( final JournaledProvenanceEvent event : events ) {
            maxId = event.getEventId();

            final Map<String, String> attributes = event.getAttributes();

            final Document doc = new Document();
            addField(doc, SearchableFields.FlowFileUUID, event.getFlowFileUuid(), Store.NO);
            addField(doc, SearchableFields.Filename, attributes.get(CoreAttributes.FILENAME.key()), Store.NO);
            addField(doc, SearchableFields.ComponentID, event.getComponentId(), Store.NO);
            addField(doc, SearchableFields.AlternateIdentifierURI, event.getAlternateIdentifierUri(), Store.NO);
            addField(doc, SearchableFields.EventType, event.getEventType().name(), Store.NO);
            addField(doc, SearchableFields.Relationship, event.getRelationship(), Store.NO);
            addField(doc, SearchableFields.Details, event.getDetails(), Store.NO);
            addField(doc, SearchableFields.ContentClaimSection, event.getContentClaimSection(), Store.NO);
            addField(doc, SearchableFields.ContentClaimContainer, event.getContentClaimContainer(), Store.NO);
            addField(doc, SearchableFields.ContentClaimIdentifier, event.getContentClaimIdentifier(), Store.NO);
            addField(doc, SearchableFields.SourceQueueIdentifier, event.getSourceQueueIdentifier(), Store.NO);

            if (nonAttributeSearchableFields.contains(SearchableFields.TransitURI)) {
                addField(doc, SearchableFields.TransitURI, event.getTransitUri(), Store.NO);
            }

            for (final SearchableField searchableField : attributeSearchableFields) {
                addField(doc, searchableField, attributes.get(searchableField.getSearchableFieldName()), Store.NO);
            }

            // Index the fields that we always index (unless there's nothing else to index at all)
            doc.add(new LongField(SearchableFields.LineageStartDate.getSearchableFieldName(), event.getLineageStartDate(), Store.NO));
            doc.add(new LongField(SearchableFields.EventTime.getSearchableFieldName(), event.getEventTime(), Store.NO));
            doc.add(new LongField(SearchableFields.FileSize.getSearchableFieldName(), event.getFileSize(), Store.NO));

            final JournaledStorageLocation location = event.getStorageLocation();
            doc.add(new StringField(IndexedFieldNames.CONTAINER_NAME, location.getContainerName(), Store.YES));
            doc.add(new StringField(IndexedFieldNames.SECTION_NAME, location.getSectionName(), Store.YES));
            doc.add(new StringField(IndexedFieldNames.JOURNAL_ID, location.getJournalId(), Store.YES));
            doc.add(new LongField(IndexedFieldNames.BLOCK_INDEX, location.getBlockIndex(), Store.YES));
            doc.add(new LongField(IndexedFieldNames.EVENT_ID, location.getEventId(), Store.YES));

            for (final String lineageIdentifier : event.getLineageIdentifiers()) {
                addField(doc, SearchableFields.LineageIdentifier, lineageIdentifier, Store.NO);
            }

            // If it's event is a FORK, or JOIN, add the FlowFileUUID for all child/parent UUIDs.
            if (event.getEventType() == ProvenanceEventType.FORK || event.getEventType() == ProvenanceEventType.CLONE || event.getEventType() == ProvenanceEventType.REPLAY) {
                for (final String uuid : event.getChildUuids()) {
                    if (!uuid.equals(event.getFlowFileUuid())) {
                        addField(doc, SearchableFields.FlowFileUUID, uuid, Store.NO);
                    }
                }
            } else if (event.getEventType() == ProvenanceEventType.JOIN) {
                for (final String uuid : event.getParentUuids()) {
                    if (!uuid.equals(event.getFlowFileUuid())) {
                        addField(doc, SearchableFields.FlowFileUUID, uuid, Store.NO);
                    }
                }
            } else if (event.getEventType() == ProvenanceEventType.RECEIVE && event.getSourceSystemFlowFileIdentifier() != null) {
                // If we get a receive with a Source System FlowFile Identifier, we add another Document that shows the UUID
                // that the Source System uses to refer to the data.
                final String sourceIdentifier = event.getSourceSystemFlowFileIdentifier();
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

        // Update the index's max id
        boolean updated = false;
        do {
            long curMax = indexMaxId.get();
            if ( maxId > curMax ) {
                updated = indexMaxId.compareAndSet(curMax, maxId);
            } else {
                updated = true;
            }
        } while (!updated);
    }
    
    
    @Override
    public void delete(final String containerName, final String section, final String journalId) throws IOException {
        final BooleanQuery query = new BooleanQuery();
        query.add(new BooleanClause(new TermQuery(new Term(IndexedFieldNames.CONTAINER_NAME, containerName)), Occur.MUST));
        query.add(new BooleanClause(new TermQuery(new Term(IndexedFieldNames.SECTION_NAME, section)), Occur.MUST));
        query.add(new BooleanClause(new TermQuery(new Term(IndexedFieldNames.JOURNAL_ID, journalId)), Occur.MUST));
        
        indexWriter.deleteDocuments(query);
    }
    
    
    @Override
    public void sync() throws IOException {
        indexWriter.commit();
    }
}
