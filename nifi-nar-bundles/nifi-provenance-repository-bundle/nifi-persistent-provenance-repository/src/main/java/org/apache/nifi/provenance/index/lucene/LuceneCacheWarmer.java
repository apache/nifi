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

package org.apache.nifi.provenance.index.lucene;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.provenance.index.EventIndexSearcher;
import org.apache.nifi.provenance.lucene.IndexManager;
import org.apache.nifi.provenance.util.DirectoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneCacheWarmer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(LuceneCacheWarmer.class);

    private final File storageDir;
    private final IndexManager indexManager;

    public LuceneCacheWarmer(final File storageDir, final IndexManager indexManager) {
        this.storageDir = storageDir;
        this.indexManager = indexManager;
    }

    @Override
    public void run() {
        try {
            final File[] indexDirs = storageDir.listFiles(DirectoryUtils.INDEX_FILE_FILTER);
            if (indexDirs == null) {
                logger.info("Cannot warm Lucene Index Cache for " + storageDir + " because the directory could not be read");
                return;
            }

            logger.info("Beginning warming of Lucene Index Cache for " + storageDir);
            final long startNanos = System.nanoTime();
            for (final File indexDir : indexDirs) {
                final long indexStartNanos = System.nanoTime();

                final EventIndexSearcher eventSearcher = indexManager.borrowIndexSearcher(indexDir);
                indexManager.returnIndexSearcher(eventSearcher);

                final long indexWarmMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - indexStartNanos);
                logger.debug("Took {} ms to warm Lucene Index {}", indexWarmMillis, indexDir);
            }

            final long warmSecs = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos);
            logger.info("Finished warming all Lucene Indexes for {} in {} seconds", storageDir, warmSecs);
        } catch (final Exception e) {
            logger.error("Failed to warm Lucene Index Cache for " + storageDir, e);
        }
    }
}
