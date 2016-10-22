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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

public class CachingIndexManager implements Closeable, IndexManager {
    private static final Logger logger = LoggerFactory.getLogger(CachingIndexManager.class);

    private final Lock lock = new ReentrantLock();
    private final Map<File, IndexWriterCount> writerCounts = new HashMap<>();
    private final Map<File, List<ActiveIndexSearcher>> activeSearchers = new HashMap<>();


    public void removeIndex(final File indexDirectory) {
        final File absoluteFile = indexDirectory.getAbsoluteFile();
        logger.info("Removing index {}", indexDirectory);

        lock.lock();
        try {
            final IndexWriterCount count = writerCounts.remove(absoluteFile);
            if ( count != null ) {
                try {
                    count.close();
                } catch (final IOException ioe) {
                    logger.warn("Failed to close Index Writer {} for {}", count.getWriter(), absoluteFile);
                    if ( logger.isDebugEnabled() ) {
                        logger.warn("", ioe);
                    }
                }
            }

            final List<ActiveIndexSearcher> searcherList = activeSearchers.remove(absoluteFile);
            if (searcherList != null) {
                for ( final ActiveIndexSearcher searcher : searcherList ) {
                    try {
                        searcher.close();
                    } catch (final IOException ioe) {
                        logger.warn("Failed to close Index Searcher {} for {} due to {}",
                                searcher.getSearcher(), absoluteFile, ioe);
                        if ( logger.isDebugEnabled() ) {
                            logger.warn("", ioe);
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public IndexWriter borrowIndexWriter(final File indexingDirectory) throws IOException {
        final File absoluteFile = indexingDirectory.getAbsoluteFile();
        logger.trace("Borrowing index writer for {}", indexingDirectory);

        lock.lock();
        try {
            IndexWriterCount writerCount = writerCounts.remove(absoluteFile);
            if ( writerCount == null ) {
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
                    for ( final Closeable closeable : closeables ) {
                        try {
                            closeable.close();
                        } catch (final IOException ioe2) {
                            ioe.addSuppressed(ioe2);
                        }
                    }

                    throw ioe;
                }

                writerCounts.put(absoluteFile, writerCount);

                // Mark any active searchers as poisoned because we are updating the index
                final List<ActiveIndexSearcher> searchers = activeSearchers.get(absoluteFile);
                if ( searchers != null ) {
                    for (final ActiveIndexSearcher activeSearcher : searchers) {
                        logger.debug("Poisoning {} because it is searching {}, which is getting updated", activeSearcher, indexingDirectory);
                        activeSearcher.poison();
                    }
                }
            } else {
                logger.debug("Providing existing index writer for {} and incrementing count to {}", indexingDirectory, writerCount.getCount() + 1);
                writerCounts.put(absoluteFile, new IndexWriterCount(writerCount.getWriter(),
                        writerCount.getAnalyzer(), writerCount.getDirectory(), writerCount.getCount() + 1));
            }

            return writerCount.getWriter();
        } finally {
            lock.unlock();
        }
    }

    public void returnIndexWriter(final File indexingDirectory, final IndexWriter writer) {
        final File absoluteFile = indexingDirectory.getAbsoluteFile();
        logger.trace("Returning Index Writer for {} to IndexManager", indexingDirectory);

        lock.lock();
        try {
            final IndexWriterCount count = writerCounts.remove(absoluteFile);

            try {
                if ( count == null ) {
                    logger.warn("Index Writer {} was returned to IndexManager for {}, but this writer is not known. "
                            + "This could potentially lead to a resource leak", writer, indexingDirectory);
                    writer.close();
                } else if ( count.getCount() <= 1 ) {
                    // we are finished with this writer.
                    logger.debug("Decrementing count for Index Writer for {} to {}; Closing writer", indexingDirectory, count.getCount() - 1);
                    count.close();
                } else {
                    // decrement the count.
                    logger.debug("Decrementing count for Index Writer for {} to {}", indexingDirectory, count.getCount() - 1);
                    writerCounts.put(absoluteFile, new IndexWriterCount(count.getWriter(), count.getAnalyzer(), count.getDirectory(), count.getCount() - 1));
                }
            } catch (final IOException ioe) {
                logger.warn("Failed to close Index Writer {} due to {}", writer, ioe);
                if ( logger.isDebugEnabled() ) {
                    logger.warn("", ioe);
                }
            }
        } finally {
            lock.unlock();
        }
    }


    public IndexSearcher borrowIndexSearcher(final File indexDir) throws IOException {
        final File absoluteFile = indexDir.getAbsoluteFile();
        logger.trace("Borrowing index searcher for {}", indexDir);

        lock.lock();
        try {
            // check if we already have a reader cached.
            List<ActiveIndexSearcher> currentlyCached = activeSearchers.get(absoluteFile);
            if ( currentlyCached == null ) {
                currentlyCached = new ArrayList<>();
                activeSearchers.put(absoluteFile, currentlyCached);
            } else {
                // keep track of any searchers that have been closed so that we can remove them
                // from our cache later.
                for (final ActiveIndexSearcher searcher : currentlyCached) {
                    if (searcher.isCache()) {
                        // if the searcher is poisoned, we want to close and expire it.
                        if (searcher.isPoisoned()) {
                            continue;
                        }

                        // if there are no references to the reader, it will have been closed. Since there is no
                        // isClosed() method, this is how we determine whether it's been closed or not.
                        final int refCount = searcher.getSearcher().getIndexReader().getRefCount();
                        if (refCount <= 0) {
                            // if refCount == 0, then the reader has been closed, so we cannot use the searcher
                            logger.debug("Reference count for cached Index Searcher for {} is currently {}; "
                                + "removing cached searcher", absoluteFile, refCount);
                            continue;
                        }

                        final int referenceCount = searcher.incrementReferenceCount();
                        logger.debug("Providing previously cached index searcher for {} and incrementing Reference Count to {}", indexDir, referenceCount);
                        return searcher.getSearcher();
                    }
                }
            }

            // We found no cached Index Readers. Create a new one. To do this, we need to check
            // if we have an Index Writer, and if so create a Reader based on the Index Writer.
            // This will provide us a 'near real time' index reader.
            final IndexWriterCount writerCount = writerCounts.remove(absoluteFile);
            if ( writerCount == null ) {
                final Directory directory = FSDirectory.open(absoluteFile);
                logger.debug("No Index Writer currently exists for {}; creating a cachable reader", indexDir);

                try {
                    final DirectoryReader directoryReader = DirectoryReader.open(directory);
                    final IndexSearcher searcher = new IndexSearcher(directoryReader);

                    // we want to cache the searcher that we create, since it's just a reader.
                    final ActiveIndexSearcher cached = new ActiveIndexSearcher(searcher, absoluteFile, directoryReader, directory, true);
                    currentlyCached.add(cached);

                    return cached.getSearcher();
                } catch (final IOException e) {
                    logger.error("Failed to create Index Searcher for {} due to {}", absoluteFile, e.toString());
                    logger.error("", e);

                    try {
                        directory.close();
                    } catch (final IOException ioe) {
                        e.addSuppressed(ioe);
                    }

                    throw e;
                }
            } else {
                logger.debug("Index Writer currently exists for {}; creating a non-cachable reader and incrementing "
                        + "counter to {}", indexDir, writerCount.getCount() + 1);

                // increment the writer count to ensure that it's kept open.
                writerCounts.put(absoluteFile, new IndexWriterCount(writerCount.getWriter(),
                        writerCount.getAnalyzer(), writerCount.getDirectory(), writerCount.getCount() + 1));

                // create a new Index Searcher from the writer so that we don't have an issue with trying
                // to read from a directory that's locked. If we get the "no segments* file found" with
                // Lucene, this indicates that an IndexWriter already has the directory open.
                final IndexWriter writer = writerCount.getWriter();
                final DirectoryReader directoryReader = DirectoryReader.open(writer, false);
                final IndexSearcher searcher = new IndexSearcher(directoryReader);

                // we don't want to cache this searcher because it's based on a writer, so we want to get
                // new values the next time that we search.
                final ActiveIndexSearcher activeSearcher = new ActiveIndexSearcher(searcher, absoluteFile, directoryReader, null, false);

                currentlyCached.add(activeSearcher);
                return activeSearcher.getSearcher();
            }
        } finally {
            lock.unlock();
        }
    }


    public void returnIndexSearcher(final File indexDirectory, final IndexSearcher searcher) {
        final File absoluteFile = indexDirectory.getAbsoluteFile();
        logger.trace("Returning index searcher for {} to IndexManager", indexDirectory);

        lock.lock();
        try {
            // check if we already have a reader cached.
            final List<ActiveIndexSearcher> currentlyCached = activeSearchers.get(absoluteFile);
            if ( currentlyCached == null ) {
                logger.warn("Received Index Searcher for {} but no searcher was provided for that directory; this could "
                        + "result in a resource leak", indexDirectory);
                return;
            }

            // Check if the given searcher is in our list. We use an Iterator to do this so that if we
            // find it we can call remove() on the iterator if need be.
            final Iterator<ActiveIndexSearcher> itr = new ArrayList<>(currentlyCached).iterator();
            boolean activeSearcherFound = false;
            while (itr.hasNext()) {
                final ActiveIndexSearcher activeSearcher = itr.next();
                if ( activeSearcher.getSearcher().equals(searcher) ) {
                    activeSearcherFound = true;
                    if ( activeSearcher.isCache() ) {
                        // if the searcher is poisoned, close it and remove from "pool". Otherwise,
                        // just decrement the count. Note here that when we call close() it won't actually close
                        // the underlying directory reader unless there are no more references to it
                        if ( activeSearcher.isPoisoned() ) {
                            itr.remove();

                            try {
                                activeSearcher.close();
                            } catch (final IOException ioe) {
                                logger.warn("Failed to close Index Searcher for {} due to {}", absoluteFile, ioe);
                                if ( logger.isDebugEnabled() ) {
                                    logger.warn("", ioe);
                                }
                            }

                            return;
                        } else {
                            // the searcher is cached. Just leave it open.
                            final int refCount = activeSearcher.decrementReferenceCount();
                            logger.debug("Index searcher for {} is cached; leaving open with reference count of {}", indexDirectory, refCount);
                            return;
                        }
                    } else {
                        // searcher is not cached. It was created from a writer, and we want
                        // the newest updates the next time that we get a searcher, so we will
                        // go ahead and close this one out.
                        itr.remove();

                        // decrement the writer count because we incremented it when creating the searcher
                        final IndexWriterCount writerCount = writerCounts.remove(absoluteFile);
                        if ( writerCount != null ) {
                            if ( writerCount.getCount() <= 1 ) {
                                try {
                                    logger.debug("Index searcher for {} is not cached. Writer count is "
                                            + "decremented to {}; closing writer", indexDirectory, writerCount.getCount() - 1);

                                    writerCount.close();
                                } catch (final IOException ioe) {
                                    logger.warn("Failed to close Index Writer for {} due to {}", absoluteFile, ioe);
                                    if ( logger.isDebugEnabled() ) {
                                        logger.warn("", ioe);
                                    }
                                }
                            } else {
                                logger.debug("Index searcher for {} is not cached. Writer count is decremented "
                                        + "to {}; leaving writer open", indexDirectory, writerCount.getCount() - 1);

                                writerCounts.put(absoluteFile, new IndexWriterCount(writerCount.getWriter(),
                                        writerCount.getAnalyzer(), writerCount.getDirectory(),
                                        writerCount.getCount() - 1));
                            }
                        }

                        try {
                            logger.debug("Closing Index Searcher for {}", indexDirectory);
                            final boolean allReferencesClosed = activeSearcher.close();
                            if (!allReferencesClosed) {
                                currentlyCached.add(activeSearcher);
                            }
                        } catch (final IOException ioe) {
                            logger.warn("Failed to close Index Searcher for {} due to {}", absoluteFile, ioe);
                            if ( logger.isDebugEnabled() ) {
                                logger.warn("", ioe);
                            }
                        }
                    }
                }
            }

            if (!activeSearcherFound) {
                logger.debug("Index Searcher {} was returned for {} but found no Active Searcher for it. "
                    + "This will occur if the Index Searcher was already returned while being poisoned.", searcher, indexDirectory);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        logger.debug("Closing Index Manager");

        lock.lock();
        try {
            IOException ioe = null;

            for ( final IndexWriterCount count : writerCounts.values() ) {
                try {
                    count.close();
                } catch (final IOException e) {
                    if ( ioe == null ) {
                        ioe = e;
                    } else {
                        ioe.addSuppressed(e);
                    }
                }
            }

            for (final List<ActiveIndexSearcher> searcherList : activeSearchers.values()) {
                for (final ActiveIndexSearcher searcher : searcherList) {
                    try {
                        searcher.close();
                    } catch (final IOException e) {
                        if ( ioe == null ) {
                            ioe = e;
                        } else {
                            ioe.addSuppressed(e);
                        }
                    }
                }
            }

            if ( ioe != null ) {
                throw ioe;
            }
        } finally {
            lock.unlock();
        }
    }


    private static void close(final Closeable... closeables) throws IOException {
        IOException ioe = null;
        for ( final Closeable closeable : closeables ) {
            if ( closeable == null ) {
                continue;
            }

            try {
                closeable.close();
            } catch (final IOException e) {
                if ( ioe == null ) {
                    ioe = e;
                } else {
                    ioe.addSuppressed(e);
                }
            }
        }

        if ( ioe != null ) {
            throw ioe;
        }
    }


    private static class ActiveIndexSearcher {
        private final IndexSearcher searcher;
        private final DirectoryReader directoryReader;
        private final File indexDirectory;
        private final Directory directory;
        private final boolean cache;
        private final AtomicInteger referenceCount = new AtomicInteger(1);
        private volatile boolean poisoned = false;

        public ActiveIndexSearcher(final IndexSearcher searcher, final File indexDirectory, final DirectoryReader directoryReader,
                final Directory directory, final boolean cache) {
            this.searcher = searcher;
            this.directoryReader = directoryReader;
            this.indexDirectory = indexDirectory;
            this.directory = directory;
            this.cache = cache;
        }

        public boolean isCache() {
            return cache;
        }

        public IndexSearcher getSearcher() {
            return searcher;
        }

        public boolean isPoisoned() {
            return poisoned;
        }

        public void poison() {
            this.poisoned = true;
        }

        public int incrementReferenceCount() {
            return referenceCount.incrementAndGet();
        }

        public int decrementReferenceCount() {
            return referenceCount.decrementAndGet();
        }

        public boolean close() throws IOException {
            final int updatedRefCount = referenceCount.decrementAndGet();
            if (updatedRefCount <= 0) {
                logger.debug("Decremented Reference Count for {} to {}; closing underlying directory reader", this, updatedRefCount);
                CachingIndexManager.close(directoryReader, directory);
                return true;
            } else {
                logger.debug("Decremented Reference Count for {} to {}; leaving underlying directory reader open", this, updatedRefCount);
                return false;
            }
        }

        @Override
        public String toString() {
            return "ActiveIndexSearcher[directory=" + indexDirectory + ", cached=" + cache + ", poisoned=" + poisoned + "]";
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
            CachingIndexManager.close(writer, analyzer, directory);
        }
    }

}
