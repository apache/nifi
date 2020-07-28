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

package org.apache.nifi.provenance.store;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.EventIdFirstSchemaRecordWriter;
import org.apache.nifi.provenance.IdentifierLookup;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.TestUtil;
import org.apache.nifi.provenance.index.EventIndex;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.provenance.util.DirectoryUtils;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestWriteAheadStorePartition {

    @Test
    @SuppressWarnings("unchecked")
    public void testReindex() throws IOException {
        final RepositoryConfiguration repoConfig = createConfig(1, "testReindex");
        repoConfig.setMaxEventFileCount(5);

        final String partitionName = repoConfig.getStorageDirectories().keySet().iterator().next();
        final File storageDirectory = repoConfig.getStorageDirectories().values().iterator().next();

        final RecordWriterFactory recordWriterFactory = (file, idGenerator, compressed, createToc) -> {
            final TocWriter tocWriter = createToc ? new StandardTocWriter(TocUtil.getTocFile(file), false, false) : null;
            return new EventIdFirstSchemaRecordWriter(file, idGenerator, tocWriter, compressed, 32 * 1024, IdentifierLookup.EMPTY);
        };

        final RecordReaderFactory recordReaderFactory = RecordReaders::newRecordReader;

        final WriteAheadStorePartition partition = new WriteAheadStorePartition(storageDirectory, partitionName, repoConfig, recordWriterFactory,
            recordReaderFactory, new LinkedBlockingQueue<>(), new AtomicLong(0L), EventReporter.NO_OP, Mockito.mock(EventFileManager.class));

        for (int i = 0; i < 100; i++) {
            partition.addEvents(Collections.singleton(TestUtil.createEvent()));
        }

        final Map<ProvenanceEventRecord, StorageSummary> reindexedEvents = new ConcurrentHashMap<>();
        final EventIndex eventIndex = Mockito.mock(EventIndex.class);
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                final Map<ProvenanceEventRecord, StorageSummary> events = invocation.getArgument(0);
                reindexedEvents.putAll(events);
                return null;
            }
        }).when(eventIndex).reindexEvents(Mockito.anyMap());

        Mockito.doReturn(18L).when(eventIndex).getMinimumEventIdToReindex("1");
        partition.reindexLatestEvents(eventIndex);

        final List<Long> eventIdsReindexed = reindexedEvents.values().stream()
            .map(StorageSummary::getEventId)
            .sorted()
            .collect(Collectors.toList());

        assertEquals(82, eventIdsReindexed.size());
        for (int i = 0; i < eventIdsReindexed.size(); i++) {
            assertEquals(18 + i, eventIdsReindexed.get(i).intValue());
        }
    }

    @Test
    public void testInitEmptyFile() throws IOException {
        final RepositoryConfiguration repoConfig = createConfig(1, "testInitEmptyFile");
        repoConfig.setMaxEventFileCount(5);

        final String partitionName = repoConfig.getStorageDirectories().keySet().iterator().next();
        final File storageDirectory = repoConfig.getStorageDirectories().values().iterator().next();

        final RecordWriterFactory recordWriterFactory = (file, idGenerator, compressed, createToc) -> {
            final TocWriter tocWriter = createToc ? new StandardTocWriter(TocUtil.getTocFile(file), false, false) : null;
            return new EventIdFirstSchemaRecordWriter(file, idGenerator, tocWriter, compressed, 32 * 1024, IdentifierLookup.EMPTY);
        };

        final RecordReaderFactory recordReaderFactory = RecordReaders::newRecordReader;

        WriteAheadStorePartition partition = new WriteAheadStorePartition(storageDirectory, partitionName, repoConfig, recordWriterFactory,
                recordReaderFactory, new LinkedBlockingQueue<>(), new AtomicLong(0L), EventReporter.NO_OP, Mockito.mock(EventFileManager.class));

        for (int i = 0; i < 100; i++) {
            partition.addEvents(Collections.singleton(TestUtil.createEvent()));
        }

        long maxEventId = partition.getMaxEventId();
        assertTrue(maxEventId > 0);
        partition.close();

        final List<File> fileList = Arrays.asList(storageDirectory.listFiles(DirectoryUtils.EVENT_FILE_FILTER));
        Collections.sort(fileList, DirectoryUtils.LARGEST_ID_FIRST);

        // Create new empty prov file with largest id
        assertTrue(new File(storageDirectory, "1" + fileList.get(0).getName()).createNewFile());

        partition = new WriteAheadStorePartition(storageDirectory, partitionName, repoConfig, recordWriterFactory,
                recordReaderFactory, new LinkedBlockingQueue<>(), new AtomicLong(0L), EventReporter.NO_OP, Mockito.mock(EventFileManager.class));

        partition.initialize();

        assertEquals(maxEventId, partition.getMaxEventId());
    }

    private RepositoryConfiguration createConfig(final int numStorageDirs, final String testName) {
        final RepositoryConfiguration config = new RepositoryConfiguration();
        final File storageDir = new File("target/storage/" + testName + "/" + UUID.randomUUID().toString());

        for (int i = 1; i <= numStorageDirs; i++) {
            config.addStorageDirectory(String.valueOf(1), new File(storageDir, String.valueOf(i)));
        }

        config.setJournalCount(4);
        return config;
    }
}
