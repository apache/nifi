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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.provenance.IndexConfiguration;
import org.apache.nifi.provenance.PersistentProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.rollover.RolloverAction;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingAction implements RolloverAction {

    private final PersistentProvenanceRepository repository;
    private final Set<SearchableField> nonAttributeSearchableFields;
    private final Set<SearchableField> attributeSearchableFields;
    private final IndexConfiguration indexConfiguration;
    private static final Logger logger = LoggerFactory.getLogger(IndexingAction.class);

    public IndexingAction(final PersistentProvenanceRepository repo, final IndexConfiguration indexConfig) {
        repository = repo;
        indexConfiguration = indexConfig;

        attributeSearchableFields = Collections.unmodifiableSet(new HashSet<>(repo.getConfiguration().getSearchableAttributes()));
        nonAttributeSearchableFields = Collections.unmodifiableSet(new HashSet<>(repo.getConfiguration().getSearchableFields()));
    }

    private void addField(final Document doc, final SearchableField field, final String value, final Store store) {
        if (value == null || (!nonAttributeSearchableFields.contains(field) && !field.isAttribute())) {
            return;
        }

        doc.add(new StringField(field.getSearchableFieldName(), value.toLowerCase(), store));
    }

    
    public void index(final StandardProvenanceEventRecord record, final IndexWriter indexWriter, final Integer blockIndex) throws IOException {
        final Map<String, String> attributes = record.getAttributes();

        final Document doc = new Document();
        addField(doc, SearchableFields.FlowFileUUID, record.getFlowFileUuid(), Store.NO);
        addField(doc, SearchableFields.Filename, attributes.get(CoreAttributes.FILENAME.key()), Store.NO);
        addField(doc, SearchableFields.ComponentID, record.getComponentId(), Store.NO);
        addField(doc, SearchableFields.AlternateIdentifierURI, record.getAlternateIdentifierUri(), Store.NO);
        addField(doc, SearchableFields.EventType, record.getEventType().name(), Store.NO);
        addField(doc, SearchableFields.Relationship, record.getRelationship(), Store.NO);
        addField(doc, SearchableFields.Details, record.getDetails(), Store.NO);
        addField(doc, SearchableFields.ContentClaimSection, record.getContentClaimSection(), Store.NO);
        addField(doc, SearchableFields.ContentClaimContainer, record.getContentClaimContainer(), Store.NO);
        addField(doc, SearchableFields.ContentClaimIdentifier, record.getContentClaimIdentifier(), Store.NO);
        addField(doc, SearchableFields.SourceQueueIdentifier, record.getSourceQueueIdentifier(), Store.NO);

        if (nonAttributeSearchableFields.contains(SearchableFields.TransitURI)) {
            addField(doc, SearchableFields.TransitURI, record.getTransitUri(), Store.NO);
        }

        for (final SearchableField searchableField : attributeSearchableFields) {
            addField(doc, searchableField, attributes.get(searchableField.getSearchableFieldName()), Store.NO);
        }

        final String storageFilename = LuceneUtil.substringBefore(record.getStorageFilename(), ".");

        // Index the fields that we always index (unless there's nothing else to index at all)
        if (!doc.getFields().isEmpty()) {
            doc.add(new LongField(SearchableFields.LineageStartDate.getSearchableFieldName(), record.getLineageStartDate(), Store.NO));
            doc.add(new LongField(SearchableFields.EventTime.getSearchableFieldName(), record.getEventTime(), Store.NO));
            doc.add(new LongField(SearchableFields.FileSize.getSearchableFieldName(), record.getFileSize(), Store.NO));
            doc.add(new StringField(FieldNames.STORAGE_FILENAME, storageFilename, Store.YES));
            
            if ( blockIndex == null ) {
            	doc.add(new LongField(FieldNames.STORAGE_FILE_OFFSET, record.getStorageByteOffset(), Store.YES));
            } else {
	            doc.add(new IntField(FieldNames.BLOCK_INDEX, blockIndex, Store.YES));
	            doc.add(new LongField(SearchableFields.Identifier.getSearchableFieldName(), record.getEventId(), Store.YES));
            }
            
            for (final String lineageIdentifier : record.getLineageIdentifiers()) {
                addField(doc, SearchableFields.LineageIdentifier, lineageIdentifier, Store.NO);
            }

            // If it's event is a FORK, or JOIN, add the FlowFileUUID for all child/parent UUIDs.
            if (record.getEventType() == ProvenanceEventType.FORK || record.getEventType() == ProvenanceEventType.CLONE || record.getEventType() == ProvenanceEventType.REPLAY) {
                for (final String uuid : record.getChildUuids()) {
                    if (!uuid.equals(record.getFlowFileUuid())) {
                        addField(doc, SearchableFields.FlowFileUUID, uuid, Store.NO);
                    }
                }
            } else if (record.getEventType() == ProvenanceEventType.JOIN) {
                for (final String uuid : record.getParentUuids()) {
                    if (!uuid.equals(record.getFlowFileUuid())) {
                        addField(doc, SearchableFields.FlowFileUUID, uuid, Store.NO);
                    }
                }
            } else if (record.getEventType() == ProvenanceEventType.RECEIVE && record.getSourceSystemFlowFileIdentifier() != null) {
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
    
    @Override
    public File execute(final File fileRolledOver) throws IOException {
        final File indexingDirectory = indexConfiguration.getWritableIndexDirectory(fileRolledOver);
        int indexCount = 0;
        long maxId = -1L;

        try (final Directory directory = FSDirectory.open(indexingDirectory);
                final Analyzer analyzer = new StandardAnalyzer()) {

            final IndexWriterConfig config = new IndexWriterConfig(LuceneUtil.LUCENE_VERSION, analyzer);
            config.setWriteLockTimeout(300000L);

            try (final IndexWriter indexWriter = new IndexWriter(directory, config);
                    final RecordReader reader = RecordReaders.newRecordReader(fileRolledOver, repository.getAllLogFiles())) {
                StandardProvenanceEventRecord record;
                while (true) {
                	final Integer blockIndex;
                	if ( reader.isBlockIndexAvailable() ) {
                		blockIndex = reader.getBlockIndex();
                	} else {
                		blockIndex = null;
                	}
                	
                    try {
                        record = reader.nextRecord();
                    } catch (final EOFException eof) {
                        // system was restarted while writing to the log file. Nothing we can do here, so ignore this record.
                        // On system restart, the FlowFiles should be back in their "original" queues, so the events will be re-created
                        // when the data is re-processed
                        break;
                    }

                    if (record == null) {
                        break;
                    }

                    maxId = record.getEventId();

                    index(record, indexWriter, blockIndex);
                    indexCount++;
                }

                indexWriter.commit();
            } catch (final EOFException eof) {
                // nothing in the file. Move on.
            }
        } finally {
            if (maxId >= -1) {
                indexConfiguration.setMaxIdIndexed(maxId);
            }
        }

        final File newFile = new File(fileRolledOver.getParent(),
                LuceneUtil.substringBeforeLast(fileRolledOver.getName(), ".")
                + ".indexed."
                + LuceneUtil.substringAfterLast(fileRolledOver.getName(), "."));

        boolean renamed = false;
        for (int i = 0; i < 10 && !renamed; i++) {
            renamed = fileRolledOver.renameTo(newFile);
            if (!renamed) {
                try {
                    Thread.sleep(25L);
                } catch (final InterruptedException e) {
                }
            }
        }

        if (renamed) {
            logger.info("Finished indexing Provenance Log File {} to index {} with {} records indexed and renamed file to {}",
                    fileRolledOver, indexingDirectory, indexCount, newFile);
            return newFile;
        } else {
            logger.warn("Finished indexing Provenance Log File {} with {} records indexed but failed to rename file to {}; indexed {} records", new Object[]{fileRolledOver, indexCount, newFile, indexCount});
            return fileRolledOver;
        }
    }

    @Override
    public boolean hasBeenPerformed(final File fileRolledOver) {
        return fileRolledOver.getName().contains(".indexed.");
    }
}
