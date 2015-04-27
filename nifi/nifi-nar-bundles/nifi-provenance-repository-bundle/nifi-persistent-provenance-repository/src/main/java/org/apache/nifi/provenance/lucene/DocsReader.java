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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocsReader {
	private final Logger logger = LoggerFactory.getLogger(DocsReader.class);
	
    public DocsReader(final List<File> storageDirectories) {
    }

    public Set<ProvenanceEventRecord> read(final TopDocs topDocs, final IndexReader indexReader, final Collection<Path> allProvenanceLogFiles, final AtomicInteger retrievalCount, final int maxResults) throws IOException {
        if (retrievalCount.get() >= maxResults) {
            return Collections.emptySet();
        }

        final long start = System.nanoTime();
        final int numDocs = Math.min(topDocs.scoreDocs.length, maxResults);
        final List<Document> docs = new ArrayList<>(numDocs);

        for (final ScoreDoc scoreDoc : topDocs.scoreDocs) {
            final int docId = scoreDoc.doc;
            final Document d = indexReader.document(docId);
            docs.add(d);
            if ( retrievalCount.incrementAndGet() >= maxResults ) {
                break;
            }
        }

        final long readDocuments = System.nanoTime() - start;
        logger.debug("Reading {} Lucene Documents took {} millis", docs.size(), TimeUnit.NANOSECONDS.toMillis(readDocuments));
        return read(docs, allProvenanceLogFiles);
    }

    
    private long getByteOffset(final Document d, final RecordReader reader) {
        final IndexableField blockField = d.getField(FieldNames.BLOCK_INDEX);
        if ( blockField != null ) {
        	final int blockIndex = blockField.numericValue().intValue();
        	final TocReader tocReader = reader.getTocReader();
        	return tocReader.getBlockOffset(blockIndex);
        }
        
    	return d.getField(FieldNames.STORAGE_FILE_OFFSET).numericValue().longValue();
    }
    
    
    private ProvenanceEventRecord getRecord(final Document d, final RecordReader reader) throws IOException {
    	IndexableField blockField = d.getField(FieldNames.BLOCK_INDEX);
    	if ( blockField == null ) {
    		reader.skipTo(getByteOffset(d, reader));
    	} else {
    		reader.skipToBlock(blockField.numericValue().intValue());
    	}
    	
        StandardProvenanceEventRecord record;
        while ( (record = reader.nextRecord()) != null) {
        	IndexableField idField = d.getField(SearchableFields.Identifier.getSearchableFieldName());
        	if ( idField == null || idField.numericValue().longValue() == record.getEventId() ) {
        		break;
        	}
        }
        
        if ( record == null ) {
        	throw new IOException("Failed to find Provenance Event " + d);
        } else {
        	return record;
        }
    }
    

    public Set<ProvenanceEventRecord> read(final List<Document> docs, final Collection<Path> allProvenanceLogFiles) throws IOException {
        LuceneUtil.sortDocsForRetrieval(docs);

        RecordReader reader = null;
        String lastStorageFilename = null;
        final Set<ProvenanceEventRecord> matchingRecords = new LinkedHashSet<>();

        final long start = System.nanoTime();
        int logFileCount = 0;
        
        final Set<String> storageFilesToSkip = new HashSet<>();
        
        try {
            for (final Document d : docs) {
                final String storageFilename = d.getField(FieldNames.STORAGE_FILENAME).stringValue();
                if ( storageFilesToSkip.contains(storageFilename) ) {
                	continue;
                }
                
                try {
                    if (reader != null && storageFilename.equals(lastStorageFilename)) {
                       	matchingRecords.add(getRecord(d, reader));
                    } else {
                    	logger.debug("Opening log file {}", storageFilename);
                    	
                    	logFileCount++;
                        if (reader != null) {
                            reader.close();
                        }

                        List<File> potentialFiles = LuceneUtil.getProvenanceLogFiles(storageFilename, allProvenanceLogFiles);
                        if (potentialFiles.isEmpty()) {
                            logger.warn("Could not find Provenance Log File with basename {} in the "
                            		+ "Provenance Repository; assuming file has expired and continuing without it", storageFilename);
                            storageFilesToSkip.add(storageFilename);
                            continue;
                        }

                        if (potentialFiles.size() > 1) {
                            throw new FileNotFoundException("Found multiple Provenance Log Files with basename " + 
                            		storageFilename + " in the Provenance Repository");
                        }

                        for (final File file : potentialFiles) {
                            try {
                            	reader = RecordReaders.newRecordReader(file, allProvenanceLogFiles);
                               	matchingRecords.add(getRecord(d, reader));
                            } catch (final IOException e) {
                                throw new IOException("Failed to retrieve record " + d + " from Provenance File " + file + " due to " + e, e);
                            }
                        }
                    }
                } finally {
                    lastStorageFilename = storageFilename;
                }
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.debug("Took {} ms to read {} events from {} prov log files", millis, matchingRecords.size(), logFileCount);

        return matchingRecords;
    }

}
