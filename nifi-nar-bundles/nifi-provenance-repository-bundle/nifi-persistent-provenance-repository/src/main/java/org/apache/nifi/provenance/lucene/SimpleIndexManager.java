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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.index.EventIndexSearcher;
import org.apache.nifi.provenance.index.EventIndexWriter;
import org.apache.nifi.provenance.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleIndexManager implements IndexManager {
    private static final Logger logger = LoggerFactory.getLogger(SimpleIndexManager.class);

    private final Map<File, IndexWriterCount> writerCounts = new HashMap<>(); // guarded by synchronizing on map itself
    private final ExecutorService searchExecutor;
    private final RepositoryConfiguration repoConfig;

    public SimpleIndexManager(final RepositoryConfiguration repoConfig) {
        this.repoConfig = repoConfig;
        this.searchExecutor = Executors.newFixedThreadPool(repoConfig.getQueryThreadPoolSize(), new NamedThreadFactory("Search Lucene Index"));
    }

    @Override
    public void close() throws IOException {
        logger.debug("Shutting down SimpleIndexManager search executor");

        searchExecutor.shutdown();
        try {
            if (!searchExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                searchExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            searchExecutor.shutdownNow();
        }
    }

    @Override
    public EventIndexSearcher borrowIndexSearcher(final File indexDir) throws IOException {
        final File absoluteFile = indexDir.getAbsoluteFile();

        final IndexWriterCount writerCount;
        synchronized (writerCounts) {
            writerCount = writerCounts.remove(absoluteFile);

            if (writerCount != null) {
                // Increment writer count and create an Index Searcher based on the writer
                writerCounts.put(absoluteFile, new IndexWriterCount(writerCount.getWriter(), writerCount.getAnalyzer(),
                    writerCount.getDirectory(), writerCount.getCount() + 1, writerCount.isCloseableWhenUnused()));
            }
        }

        final DirectoryReader directoryReader;
        if (writerCount == null) {
            logger.trace("Creating index searcher for {}", indexDir);
            final Directory directory = FSDirectory.open(indexDir);
            directoryReader = DirectoryReader.open(directory);
        } else {
            final EventIndexWriter eventIndexWriter = writerCount.getWriter();
            directoryReader = DirectoryReader.open(eventIndexWriter.getIndexWriter(), false);
        }

        final IndexSearcher searcher = new IndexSearcher(directoryReader, this.searchExecutor);

        logger.trace("Created index searcher {} for {}", searcher, indexDir);
        return new LuceneEventIndexSearcher(searcher, indexDir, null, directoryReader);
    }

    @Override
    public void returnIndexSearcher(final EventIndexSearcher searcher) {
        final File indexDirectory = searcher.getIndexDirectory();
        logger.debug("Closing index searcher {} for {}", searcher, indexDirectory);
        closeQuietly(searcher);
        logger.debug("Closed index searcher {}", searcher);

        final IndexWriterCount count;
        boolean closeWriter = false;
        synchronized (writerCounts) {
            final File absoluteFile = searcher.getIndexDirectory().getAbsoluteFile();
            count = writerCounts.get(absoluteFile);
            if (count == null) {
                logger.debug("Returning EventIndexSearcher for {}; there is no active writer for this searcher so will not decrement writerCounts", absoluteFile);
                return;
            }

            if (count.getCount() <= 1) {
                // we are finished with this writer.
                final boolean close = count.isCloseableWhenUnused();
                logger.debug("Decrementing count for Index Writer for {} to {}{}", indexDirectory, count.getCount() - 1, close ? "; closing writer" : "");

                if (close) {
                    writerCounts.remove(absoluteFile);
                    closeWriter = true;
                } else {
                    writerCounts.put(absoluteFile, new IndexWriterCount(count.getWriter(), count.getAnalyzer(), count.getDirectory(),
                        count.getCount() - 1, count.isCloseableWhenUnused()));
                }
            } else {
                writerCounts.put(absoluteFile, new IndexWriterCount(count.getWriter(), count.getAnalyzer(), count.getDirectory(),
                    count.getCount() - 1, count.isCloseableWhenUnused()));
            }
        }

        if (closeWriter) {
            try {
                close(count);
            } catch (final Exception e) {
                logger.warn("Failed to close Index Writer {} due to {}", count.getWriter(), e.toString(), e);
            }
        }
    }

    @Override
    public boolean removeIndex(final File indexDirectory) {
        final File absoluteFile = indexDirectory.getAbsoluteFile();
        logger.debug("Attempting to remove index {} from SimpleIndexManager", absoluteFile);

        IndexWriterCount writerCount;
        synchronized (writerCounts) {
            writerCount = writerCounts.remove(absoluteFile);
            if (writerCount == null) {
                logger.debug("Allowing removal of index {} because there is no IndexWriterCount for this directory", absoluteFile);
                return true; // return true since directory has no writers
            }

            if (writerCount.getCount() > 0) {
                logger.debug("Not allowing removal of index {} because the active writer count for this directory is {}", absoluteFile, writerCount.getCount());
                writerCounts.put(absoluteFile, writerCount);
                return false;
            }
        }

        try {
            logger.debug("Removing index {} from SimpleIndexManager and closing the writer", absoluteFile);

            close(writerCount);
        } catch (final Exception e) {
            logger.error("Failed to close Index Writer for {} while removing Index from the repository;"
                + "this directory may need to be cleaned up manually.", e);
        }

        return true;
    }


    private IndexWriterCount createWriter(final File indexDirectory) throws IOException {
        final List<Closeable> closeables = new ArrayList<>();
        final Directory directory = FSDirectory.open(indexDirectory);
        closeables.add(directory);

        try {
            final Analyzer analyzer = new StandardAnalyzer();
            closeables.add(analyzer);

            final IndexWriterConfig config = new IndexWriterConfig(LuceneUtil.LUCENE_VERSION, analyzer);

            final ConcurrentMergeScheduler mergeScheduler = new ConcurrentMergeScheduler();
            final int mergeThreads = repoConfig.getConcurrentMergeThreads();
            mergeScheduler.setMaxMergesAndThreads(mergeThreads, mergeThreads);
            config.setMergeScheduler(mergeScheduler);

            final IndexWriter indexWriter = new IndexWriter(directory, config);
            final EventIndexWriter eventIndexWriter = new LuceneEventIndexWriter(indexWriter, indexDirectory);

            final IndexWriterCount writerCount = new IndexWriterCount(eventIndexWriter, analyzer, directory, 1, false);
            logger.debug("Providing new index writer for {}", indexDirectory);
            return writerCount;
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
    }

    @Override
    public EventIndexWriter borrowIndexWriter(final File indexDirectory) throws IOException {
        final File absoluteFile = indexDirectory.getAbsoluteFile();
        logger.trace("Borrowing index writer for {}", indexDirectory);

        IndexWriterCount writerCount = null;
        synchronized (writerCounts) {
            writerCount = writerCounts.get(absoluteFile);

            if (writerCount == null) {
                writerCount = createWriter(indexDirectory);
                writerCounts.put(absoluteFile, writerCount);
            } else {
                logger.trace("Providing existing index writer for {} and incrementing count to {}", indexDirectory, writerCount.getCount() + 1);
                writerCounts.put(absoluteFile, new IndexWriterCount(writerCount.getWriter(),
                    writerCount.getAnalyzer(), writerCount.getDirectory(), writerCount.getCount() + 1, writerCount.isCloseableWhenUnused()));
            }

            if (writerCounts.size() > repoConfig.getStorageDirectories().size() * 2) {
                logger.debug("Index Writer returned; writer count map now has size {}; writerCount = {}; full writerCounts map = {}",
                    writerCounts.size(), writerCount, writerCounts);
            }
        }

        return writerCount.getWriter();
    }

    @Override
    public void returnIndexWriter(final EventIndexWriter writer) {
        returnIndexWriter(writer, true, true);
    }

    @Override
    public void returnIndexWriter(final EventIndexWriter writer, final boolean commit, final boolean isCloseable) {
        final File indexDirectory = writer.getDirectory();
        final File absoluteFile = indexDirectory.getAbsoluteFile();
        logger.trace("Returning Index Writer for {} to IndexManager", indexDirectory);

        boolean unused = false;
        IndexWriterCount count = null;
        boolean close = isCloseable;
        try {
            synchronized (writerCounts) {
                count = writerCounts.get(absoluteFile);
                if (count != null && count.isCloseableWhenUnused()) {
                    close = true;
                }

                if (count == null) {
                    logger.warn("Index Writer {} was returned to IndexManager for {}, but this writer is not known. "
                        + "This could potentially lead to a resource leak", writer, indexDirectory);
                    writer.close();
                } else if (count.getCount() <= 1) {
                    // we are finished with this writer.
                    unused = true;
                    if (close) {
                        logger.debug("Decrementing count for Index Writer for {} to {}; closing writer", indexDirectory, count.getCount() - 1);
                        writerCounts.remove(absoluteFile);
                    } else {
                        logger.trace("Decrementing count for Index Writer for {} to {}", indexDirectory, count.getCount() - 1);

                        // If writer is not closeable, then we need to decrement its count.
                        writerCounts.put(absoluteFile, new IndexWriterCount(count.getWriter(), count.getAnalyzer(), count.getDirectory(),
                            count.getCount() - 1, close));
                    }
                } else {
                    // decrement the count.
                    if (close) {
                        logger.debug("Decrementing count for Index Writer for {} to {} and marking as closeable when no longer in use", indexDirectory, count.getCount() - 1);
                    } else {
                        logger.trace("Decrementing count for Index Writer for {} to {}", indexDirectory, count.getCount() - 1);
                    }

                    writerCounts.put(absoluteFile, new IndexWriterCount(count.getWriter(), count.getAnalyzer(),
                        count.getDirectory(), count.getCount() - 1, close));
                }

                if (writerCounts.size() > repoConfig.getStorageDirectories().size() * 2) {
                    logger.debug("Index Writer returned; writer count map now has size {}; writer = {}, commit = {}, isCloseable = {}, writerCount = {}; full writerCounts Map = {}",
                        writerCounts.size(), writer, commit, isCloseable, count, writerCounts);
                }
            }

            // Committing and closing are very expensive, so we want to do those outside of the synchronized block.
            // So we use an 'unused' variable to tell us whether or not we should actually do so.
            if (unused) {
                try {
                    if (commit) {
                        writer.commit();
                    }
                } finally {
                    if (close) {
                        logger.info("Index Writer for {} has been returned to Index Manager and is no longer in use. Closing Index Writer", indexDirectory);
                        close(count);
                    }
                }
            }
        } catch (final Exception e) {
            logger.warn("Failed to close Index Writer {} due to {}", writer, e.toString(), e);
        }
    }

    // This method exists solely for unit testing purposes.
    protected void close(final IndexWriterCount count) throws IOException {
        count.close();
    }

    protected int getWriterCount() {
        synchronized (writerCounts) {
            return writerCounts.size();
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


    protected static class IndexWriterCount implements Closeable {
        private final EventIndexWriter writer;
        private final Analyzer analyzer;
        private final Directory directory;
        private final int count;
        private final boolean closeableWhenUnused;

        public IndexWriterCount(final EventIndexWriter writer, final Analyzer analyzer, final Directory directory, final int count, final boolean closeableWhenUnused) {
            this.writer = writer;
            this.analyzer = analyzer;
            this.directory = directory;
            this.count = count;
            this.closeableWhenUnused = closeableWhenUnused;
        }

        public boolean isCloseableWhenUnused() {
            return closeableWhenUnused;
        }

        public Analyzer getAnalyzer() {
            return analyzer;
        }

        public Directory getDirectory() {
            return directory;
        }

        public EventIndexWriter getWriter() {
            return writer;
        }

        public int getCount() {
            return count;
        }

        @Override
        public void close() throws IOException {
            closeQuietly(writer, analyzer, directory);
        }

        @Override
        public String toString() {
            return "IndexWriterCount[count=" + count + ", writer=" + writer + ", closeableWhenUnused=" + closeableWhenUnused + "]";
        }
    }
}
