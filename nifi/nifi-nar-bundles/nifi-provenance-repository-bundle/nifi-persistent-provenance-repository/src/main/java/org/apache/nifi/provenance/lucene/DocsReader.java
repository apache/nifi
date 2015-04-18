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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

public class DocsReader {

    public DocsReader(final List<File> storageDirectories) {
    }

    public Set<ProvenanceEventRecord> read(final TopDocs topDocs, final IndexReader indexReader, final Collection<Path> allProvenanceLogFiles, final AtomicInteger retrievalCount, final int maxResults) throws IOException {
        if (retrievalCount.get() >= maxResults) {
            return Collections.emptySet();
        }

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

        return read(docs, allProvenanceLogFiles);
    }

    public Set<ProvenanceEventRecord> read(final List<Document> docs, final Collection<Path> allProvenanceLogFiles) throws IOException {
        LuceneUtil.sortDocsForRetrieval(docs);

        RecordReader reader = null;
        String lastStorageFilename = null;
        long lastByteOffset = 0L;
        final Set<ProvenanceEventRecord> matchingRecords = new LinkedHashSet<>();

        try {
            for (final Document d : docs) {
                final String storageFilename = d.getField(FieldNames.STORAGE_FILENAME).stringValue();
                final long byteOffset = d.getField(FieldNames.STORAGE_FILE_OFFSET).numericValue().longValue();

                try {
                    if (reader != null && storageFilename.equals(lastStorageFilename) && byteOffset > lastByteOffset) {
                        // Still the same file and the offset is downstream.
                        try {
                            reader.skipTo(byteOffset);
                            final StandardProvenanceEventRecord record = reader.nextRecord();
                            matchingRecords.add(record);
                        } catch (final IOException e) {
                            throw new FileNotFoundException("Could not find Provenance Log File with basename " + storageFilename + " in the Provenance Repository");
                        }

                    } else {
                        if (reader != null) {
                            reader.close();
                        }

                        List<File> potentialFiles = LuceneUtil.getProvenanceLogFiles(storageFilename, allProvenanceLogFiles);
                        if (potentialFiles.isEmpty()) {
                            throw new FileNotFoundException("Could not find Provenance Log File with basename " + storageFilename + " in the Provenance Repository");
                        }

                        if (potentialFiles.size() > 1) {
                            throw new FileNotFoundException("Found multiple Provenance Log Files with basename " + storageFilename + " in the Provenance Repository");
                        }

                        for (final File file : potentialFiles) {
                            reader = RecordReaders.newRecordReader(file, allProvenanceLogFiles);

                            try {
                                reader.skip(byteOffset);

                                final StandardProvenanceEventRecord record = reader.nextRecord();
                                matchingRecords.add(record);
                            } catch (final IOException e) {
                                throw new IOException("Failed to retrieve record from Provenance File " + file + " due to " + e, e);
                            }
                        }
                    }
                } finally {
                    lastStorageFilename = storageFilename;
                    lastByteOffset = byteOffset;
                }
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

        return matchingRecords;
    }

}
