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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleIndexManager implements IndexManager {
    private static final Logger logger = LoggerFactory.getLogger(SimpleIndexManager.class);

    private final ConcurrentMap<Object, List<Closeable>> closeables = new ConcurrentHashMap<>();
    private final Map<File, IndexWriterCount> writerCounts = new HashMap<>();

    @Override
    public void close() throws IOException {
    }

    @Override
    public IndexSearcher borrowIndexSearcher(final File indexDir) throws IOException {
        logger.debug("Creating index searcher for {}", indexDir);
        final Directory directory = FSDirectory.open(indexDir);
        final DirectoryReader directoryReader = DirectoryReader.open(directory);
        final IndexSearcher searcher = new IndexSearcher(directoryReader);

        final List<Closeable> closeableList = new ArrayList<>(2);
        closeableList.add(directoryReader);
        closeableList.add(directory);
        closeables.put(searcher, closeableList);
        logger.debug("Created index searcher {} for {}", searcher, indexDir);

        return searcher;
    }

    @Override
    public void returnIndexSearcher(final File indexDirectory, final IndexSearcher searcher) {
        logger.debug("Closing index searcher {} for {}", searcher, indexDirectory);

        final List<Closeable> closeableList = closeables.get(searcher);
        if (closeableList != null) {
            for (final Closeable closeable : closeableList) {
                closeQuietly(closeable);
            }
        }

        logger.debug("Closed index searcher {}", searcher);
    }

    @Override
    public void removeIndex(final File indexDirectory) {
    }


    @Override
    public synchronized IndexWriter borrowIndexWriter(final File indexingDirectory) throws IOException {
        final File absoluteFile = indexingDirectory.getAbsoluteFile();
        logger.trace("Borrowing index writer for {}", indexingDirectory);

        IndexWriterCount writerCount = writerCounts.remove(absoluteFile);
        if (writerCount == null) {
            final List<Closeable> closeables = new ArrayList<>();
            final Directory directory = FSDirectory.open(indexingDirectory);
            closeables.add(directory);

            try {
                final Analyzer analyzer = new StandardAnalyzer();
                closeables.add(analyzer);

                final IndexWriterConfig config = new IndexWriterConfig(LuceneUtil.LUCENE_VERSION, analyzer);
                config.setWriteLockTimeout(300000L);

                final IndexWriter indexWriter = new IndexWriter(directory, config);
                writerCount = new IndexWriterCount(indexWriter, analyzer, directory, 1);
                logger.debug("Providing new index writer for {}", indexingDirectory);
            } catch (final IOException ioe) {
                for (final Closeable closeable : closeables) {
                    try {
                        closeable.close();
                    } catch (final IOException ioe2) {
                        ioe.addSuppressed(ioe2);
                    }
                }

                throw ioe;
            }

            writerCounts.put(absoluteFile, writerCount);
        } else {
            logger.debug("Providing existing index writer for {} and incrementing count to {}", indexingDirectory, writerCount.getCount() + 1);
            writerCounts.put(absoluteFile, new IndexWriterCount(writerCount.getWriter(),
                writerCount.getAnalyzer(), writerCount.getDirectory(), writerCount.getCount() + 1));
        }

        return writerCount.getWriter();
    }


    @Override
    public synchronized void returnIndexWriter(final File indexingDirectory, final IndexWriter writer) {
        final File absoluteFile = indexingDirectory.getAbsoluteFile();
        logger.trace("Returning Index Writer for {} to IndexManager", indexingDirectory);

        final IndexWriterCount count = writerCounts.remove(absoluteFile);

        try {
            if (count == null) {
                logger.warn("Index Writer {} was returned to IndexManager for {}, but this writer is not known. "
                    + "This could potentially lead to a resource leak", writer, indexingDirectory);
                writer.close();
            } else if (count.getCount() <= 1) {
                // we are finished with this writer.
                logger.debug("Decrementing count for Index Writer for {} to {}; Closing writer", indexingDirectory, count.getCount() - 1);
                writer.commit();
                count.close();
            } else {
                // decrement the count.
                logger.debug("Decrementing count for Index Writer for {} to {}", indexingDirectory, count.getCount() - 1);
                writerCounts.put(absoluteFile, new IndexWriterCount(count.getWriter(), count.getAnalyzer(), count.getDirectory(), count.getCount() - 1));
            }
        } catch (final IOException ioe) {
            logger.warn("Failed to close Index Writer {} due to {}", writer, ioe);
            if (logger.isDebugEnabled()) {
                logger.warn("", ioe);
            }
        }
    }

    private static void closeQuietly(final Closeable... closeables) {
        for (final Closeable closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            try {
                closeable.close();
            } catch (final Exception e) {
                logger.warn("Failed to close {} due to {}", closeable, e);
            }
        }
    }


    private static class IndexWriterCount implements Closeable {
        private final IndexWriter writer;
        private final Analyzer analyzer;
        private final Directory directory;
        private final int count;

        public IndexWriterCount(final IndexWriter writer, final Analyzer analyzer, final Directory directory, final int count) {
            this.writer = writer;
            this.analyzer = analyzer;
            this.directory = directory;
            this.count = count;
        }

        public Analyzer getAnalyzer() {
            return analyzer;
        }

        public Directory getDirectory() {
            return directory;
        }

        public IndexWriter getWriter() {
            return writer;
        }

        public int getCount() {
            return count;
        }

        @Override
        public void close() throws IOException {
            closeQuietly(writer, analyzer, directory);
        }
    }
}
