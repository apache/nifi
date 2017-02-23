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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.nifi.provenance.index.EventIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneEventIndexSearcher implements EventIndexSearcher {
    private static final Logger logger = LoggerFactory.getLogger(LuceneEventIndexSearcher.class);

    private final IndexSearcher indexSearcher;
    private final File indexDirectory;
    private final Directory directory;
    private final DirectoryReader directoryReader;

    // guarded by synchronizing on 'this'
    private int usageCounter = 0;
    private boolean closed = false;

    public LuceneEventIndexSearcher(final IndexSearcher indexSearcher, final File indexDirectory, final Directory directory, final DirectoryReader directoryReader) {
        this.indexSearcher = indexSearcher;
        this.indexDirectory = indexDirectory;
        this.directory = directory;
        this.directoryReader = directoryReader;
    }

    @Override
    public IndexSearcher getIndexSearcher() {
        return indexSearcher;
    }

    @Override
    public File getIndexDirectory() {
        return indexDirectory;
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (usageCounter == 0) {
            closeQuietly(directoryReader);
            closeQuietly(directory);
        }
    }

    public synchronized void incrementUsageCounter() {
        usageCounter++;
    }

    public synchronized void decrementUsageCounter() {
        usageCounter--;
        if (usageCounter == 0 && closed) {
            closeQuietly(directoryReader);
            closeQuietly(directory);
        }
    }

    private void closeQuietly(final Closeable closeable) {
        if (closeable == null) {
            return;
        }

        try {
            closeable.close();
        } catch (final Exception e) {
            logger.warn("Failed to close {} due to {}", closeable, e);
        }
    }

}
