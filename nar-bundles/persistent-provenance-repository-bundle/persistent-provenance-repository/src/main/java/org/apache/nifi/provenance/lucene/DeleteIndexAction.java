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
import java.util.List;

import org.apache.nifi.provenance.IndexConfiguration;
import org.apache.nifi.provenance.PersistentProvenanceRepository;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.expiration.ExpirationAction;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordReaders;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteIndexAction implements ExpirationAction {

    private static final Logger logger = LoggerFactory.getLogger(DeleteIndexAction.class);
    private final PersistentProvenanceRepository repository;
    private final IndexConfiguration indexConfiguration;

    public DeleteIndexAction(final PersistentProvenanceRepository repo, final IndexConfiguration indexConfiguration) {
        this.repository = repo;
        this.indexConfiguration = indexConfiguration;
    }

    @Override
    public File execute(final File expiredFile) throws IOException {
        // count the number of records and determine the max event id that we are deleting.
        long numDeleted = 0;
        long maxEventId = -1L;
        try (final RecordReader reader = RecordReaders.newRecordReader(expiredFile, repository.getAllLogFiles())) {
            try {
                StandardProvenanceEventRecord record;
                while ((record = reader.nextRecord()) != null) {
                    numDeleted++;

                    if (record.getEventId() > maxEventId) {
                        maxEventId = record.getEventId();
                    }
                }
            } catch (final EOFException eof) {
                // finished reading -- the last record was not completely written out, so it is discarded.
            }
        } catch (final EOFException eof) {
            // no data in file.
            return expiredFile;
        }

        // remove the records from the index
        final List<File> indexDirs = indexConfiguration.getIndexDirectories(expiredFile);
        for (final File indexingDirectory : indexDirs) {
            try (final Directory directory = FSDirectory.open(indexingDirectory);
                    final Analyzer analyzer = new StandardAnalyzer(LuceneUtil.LUCENE_VERSION)) {
                IndexWriterConfig config = new IndexWriterConfig(LuceneUtil.LUCENE_VERSION, analyzer);
                config.setWriteLockTimeout(300000L);

                Term term = new Term(FieldNames.STORAGE_FILENAME, LuceneUtil.substringBefore(expiredFile.getName(), "."));

                boolean deleteDir = false;
                try (final IndexWriter indexWriter = new IndexWriter(directory, config)) {
                    indexWriter.deleteDocuments(term);
                    indexWriter.commit();
                    final int docsLeft = indexWriter.numDocs();
                    deleteDir = (docsLeft <= 0);
                    logger.debug("After expiring {}, there are {} docs left for index {}", expiredFile, docsLeft, indexingDirectory);
                }

                // we've confirmed that all documents have been removed. Delete the index directory.
                if (deleteDir) {
                    indexConfiguration.removeIndexDirectory(indexingDirectory);
                    deleteDirectory(indexingDirectory);
                    logger.info("Removed empty index directory {}", indexingDirectory);
                }
            }
        }

        // Update the minimum index to 1 more than the max Event ID in this file.
        if (maxEventId > -1L) {
            indexConfiguration.setMinIdIndexed(maxEventId + 1L);
        }

        logger.info("Deleted Indices for Expired Provenance File {} from {} index files; {} documents removed", expiredFile, indexDirs.size(), numDeleted);
        return expiredFile;
    }

    private void deleteDirectory(final File dir) {
        if (dir == null || !dir.exists()) {
            return;
        }

        final File[] children = dir.listFiles();
        if (children == null) {
            return;
        }

        for (final File child : children) {
            if (child.isDirectory()) {
                deleteDirectory(child);
            } else if (!child.delete()) {
                logger.warn("Unable to remove index directory {}; this directory should be cleaned up manually", child.getAbsolutePath());
            }
        }

        if (!dir.delete()) {
            logger.warn("Unable to remove index directory {}; this directory should be cleaned up manually", dir);
        }
    }

    @Override
    public boolean hasBeenPerformed(final File expiredFile) throws IOException {
        return false;
    }
}
