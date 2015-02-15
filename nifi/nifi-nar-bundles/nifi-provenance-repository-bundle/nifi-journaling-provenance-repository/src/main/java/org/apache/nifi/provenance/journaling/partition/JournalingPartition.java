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
package org.apache.nifi.provenance.journaling.partition;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.journaling.JournaledProvenanceEvent;
import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.journaling.index.EventIndexSearcher;
import org.apache.nifi.provenance.journaling.index.EventIndexWriter;
import org.apache.nifi.provenance.journaling.index.IndexManager;
import org.apache.nifi.provenance.journaling.index.LuceneIndexSearcher;
import org.apache.nifi.provenance.journaling.index.LuceneIndexWriter;
import org.apache.nifi.provenance.journaling.index.MultiIndexSearcher;
import org.apache.nifi.provenance.journaling.index.QueryUtils;
import org.apache.nifi.provenance.journaling.io.StandardEventSerializer;
import org.apache.nifi.provenance.journaling.journals.JournalReader;
import org.apache.nifi.provenance.journaling.journals.JournalWriter;
import org.apache.nifi.provenance.journaling.journals.StandardJournalReader;
import org.apache.nifi.provenance.journaling.journals.StandardJournalWriter;
import org.apache.nifi.provenance.journaling.tasks.CompressionTask;
import org.apache.nifi.provenance.journaling.toc.StandardTocWriter;
import org.apache.nifi.provenance.journaling.toc.TocJournalReader;
import org.apache.nifi.provenance.journaling.toc.TocWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JournalingPartition implements Partition {
    private static final Logger logger = LoggerFactory.getLogger(JournalingPartition.class);
    private static final String JOURNAL_FILE_EXTENSION = ".journal";
    
    private final String containerName;
    private final int sectionIndex;
    
    private final File section;
    private final File journalsDir;
    private final JournalingRepositoryConfig config;
    private final ExecutorService executor;
    
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    
    private JournalWriter journalWriter;
    private TocWriter tocWriter;
    private int numEventsAtEndOfLastBlock = 0;
    private volatile long maxEventId = -1L;
    private volatile Long earliestEventTime = null;
    
    private final IndexManager indexManager;
    
    public JournalingPartition(final IndexManager indexManager, final String containerName, final int sectionIndex, final File sectionDir, final JournalingRepositoryConfig config, final ExecutorService executor) throws IOException {
        this.indexManager = indexManager;
        this.containerName = containerName;
        this.sectionIndex = sectionIndex;
        this.section = sectionDir;
        this.journalsDir = new File(section, "journals");
        this.config = config;
        this.executor = executor;
        
        if (!journalsDir.exists() && !journalsDir.mkdirs()) {
            throw new IOException("Could not create directory " + section);
        }
        
        if ( journalsDir.exists() && journalsDir.isFile() ) {
            throw new IOException("Could not create directory " + section + " because a file already exists with this name");
        }
    }
    
    
    public EventIndexSearcher newIndexSearcher() throws IOException {
        return indexManager.newIndexSearcher(containerName);
    }
    
    protected JournalWriter getJournalWriter(final long firstEventId) throws IOException {
        if ( config.isReadOnly() ) {
            throw new IllegalStateException("Cannot update repository because it is read-only");
        }
        
        if (isRolloverNecessary()) {
            rollover(firstEventId);
        }
        
        return journalWriter;
    }
    
    // MUST be called with writeLock or readLock held.
    private EventIndexWriter getIndexWriter() {
        return indexManager.getIndexWriter(containerName);
    }
    
    @Override
    public List<JournaledProvenanceEvent> registerEvents(final Collection<ProvenanceEventRecord> events, final long firstEventId) throws IOException {
        writeLock.lock();
        try {
            final JournalWriter writer = getJournalWriter(firstEventId);
    
            if ( !events.isEmpty() ) {
                final int eventsWritten = writer.getEventCount();
                if ( eventsWritten - numEventsAtEndOfLastBlock > config.getBlockSize() ) {
                    writer.finishBlock();
                    tocWriter.addBlockOffset(writer.getSize());
                    numEventsAtEndOfLastBlock = eventsWritten;
                    writer.beginNewBlock();
                }
            }
    
            writer.write(events, firstEventId);
            
            final List<JournaledProvenanceEvent> storedEvents = new ArrayList<>(events.size());
            long id = firstEventId;
            for (final ProvenanceEventRecord event : events) {
                final JournaledStorageLocation location = new JournaledStorageLocation(containerName, String.valueOf(sectionIndex), 
                        String.valueOf(writer.getJournalId()), tocWriter.getCurrentBlockIndex(), id++);
                final JournaledProvenanceEvent storedEvent = new JournaledProvenanceEvent(event, location);
                storedEvents.add(storedEvent);
            }
            
            final EventIndexWriter indexWriter = getIndexWriter();
            indexWriter.index(storedEvents);
            
            if ( config.isAlwaysSync() ) {
                writer.sync();
            }
            
            // update the maxEventId; we don't need a compareAndSet because the AtomicLong is modified
            // only within a write lock. But we use AtomicLong so that we 
            if ( id > maxEventId ) {
                maxEventId = id;
            }
            
            if ( earliestEventTime == null ) {
                Long earliest = null;
                for ( final ProvenanceEventRecord event : events ) {
                    if ( earliest == null || event.getEventTime() < earliest ) {
                        earliest = event.getEventTime();
                    }
                }
                
                earliestEventTime = earliest;
            }
            
            return storedEvents;
        } finally {
            writeLock.unlock();
        }
    }

    // MUST be called with either the read lock or write lock held.
    // determines whether or not we need to roll over the journal writer and toc writer.
    private boolean isRolloverNecessary() {
        if ( journalWriter == null ) {
            return true;
        }
        
        final long ageSeconds = journalWriter.getAge(TimeUnit.SECONDS);
        final long rolloverSeconds = config.getJournalRolloverPeriod(TimeUnit.SECONDS);
        if ( ageSeconds >= rolloverSeconds ) {
            return true;
        }
        
        if ( journalWriter.getSize() > config.getJournalCapacity() ) {
            return true;
        }
        
        return false;
    }
    
    // MUST be called with write lock held.
    private void rollover(final long firstEventId) throws IOException {
        // TODO: Rework how rollover works because we now have index manager!!
        
        // if we have a writer already, close it and initiate rollover actions
        if ( journalWriter != null ) {
            journalWriter.finishBlock();
            journalWriter.close();
            tocWriter.close();

            final EventIndexWriter curWriter = getIndexWriter();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        curWriter.sync();
                    } catch (final IOException e) {
                        
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            });
            
            if ( config.isCompressOnRollover() ) {
                final File finishedFile = journalWriter.getJournalFile();
                final File finishedTocFile = tocWriter.getFile();
                executor.submit(new CompressionTask(finishedFile, journalWriter.getJournalId(), finishedTocFile));
            }
        }
        
        // create new writers and reset state.
        final File journalFile = new File(journalsDir, firstEventId + JOURNAL_FILE_EXTENSION);
        journalWriter = new StandardJournalWriter(firstEventId, journalFile, false, new StandardEventSerializer());
        tocWriter = new StandardTocWriter(QueryUtils.getTocFile(journalFile), false, config.isAlwaysSync());
        tocWriter.addBlockOffset(journalWriter.getSize());
        numEventsAtEndOfLastBlock = 0;
    }
    

    private Long getJournalId(final File file) {
        long journalId;
        final int dotIndex = file.getName().indexOf(".");
        if ( dotIndex < 0 ) {
            journalId = 0L;
        } else {
            try {
                journalId = Long.parseLong(file.getName().substring(0, dotIndex));
            } catch (final NumberFormatException nfe) {
                return null;
            }
        }
        
        return journalId;
    }
    
    @Override
    public void restore() throws IOException {
        writeLock.lock();
        try {
            // delete or rename files if stopped during rollover; compress any files that haven't been compressed
            if ( !config.isReadOnly() ) {
                final File[] children = journalsDir.listFiles();
                if ( children != null ) {
                    // find the latest journal.
                    File latestJournal = null;
                    long latestJournalId = -1L;
                    
                    final List<File> journalFiles = new ArrayList<>();
                    
                    // find any journal files that either haven't been compressed or were partially compressed when
                    // we last shutdown and then restart compression.
                    for ( final File file : children ) {
                        final String filename = file.getName();
                        if ( !filename.contains(JOURNAL_FILE_EXTENSION) ) {
                            continue;
                        }
                        
                        final Long journalId = getJournalId(file);
                        if ( journalId != null && journalId > latestJournalId ) {
                            latestJournal = file;
                            latestJournalId = journalId;
                        }
                        
                        journalFiles.add(file);
                        
                        if ( !config.isCompressOnRollover() ) {
                            continue;
                        }
                        
                        if ( filename.endsWith(CompressionTask.FILE_EXTENSION) ) {
                            final File uncompressedFile = new File(journalsDir, filename.replace(CompressionTask.FILE_EXTENSION, ""));
                            if ( uncompressedFile.exists() ) {
                                // both the compressed and uncompressed version of this journal exist. The Compression Task was
                                // not complete when we shutdown. Delete the compressed journal and toc and re-start the Compression Task.
                                final File tocFile = QueryUtils.getTocFile(uncompressedFile);
                                executor.submit(new CompressionTask(uncompressedFile, getJournalId(uncompressedFile), tocFile));
                            } else {
                                // The compressed file exists but the uncompressed file does not. This means that we have finished
                                // writing the compressed file and deleted the original journal file but then shutdown before
                                // renaming the compressed file to the original filename. We can simply rename the compressed file
                                // to the original file and then address the TOC file.
                                final boolean rename = CompressionTask.rename(file, uncompressedFile);
                                if ( !rename ) {
                                    logger.warn("{} During recovery, failed to rename {} to {}", this, file, uncompressedFile);
                                    continue;
                                }
                                
                                // Check if the compressed TOC file exists. If not, we are finished.
                                // If it does exist, then we know that it is complete, as described above, so we will go
                                // ahead and replace the uncompressed version.
                                final File tocFile = QueryUtils.getTocFile(uncompressedFile);
                                final File compressedTocFile = new File(tocFile.getParentFile(), tocFile.getName() + CompressionTask.FILE_EXTENSION);
                                if ( !compressedTocFile.exists() ) {
                                    continue;
                                }
                                
                                tocFile.delete();
                                
                                final boolean renamedTocFile = CompressionTask.rename(compressedTocFile, tocFile);
                                if ( !renamedTocFile ) {
                                    logger.warn("{} During recovery, failed to rename {} to {}", this, compressedTocFile, tocFile);
                                }
                            }
                        }
                    }
                    
                    // Get the first event in the earliest journal file so that we know what the earliest time available is
                    Collections.sort(journalFiles, new Comparator<File>() {
                        @Override
                        public int compare(final File o1, final File o2) {
                            return Long.compare(getJournalId(o1), getJournalId(o2));
                        }
                    });
                    
                    for ( final File journal : journalFiles ) {
                        try (final JournalReader reader = new StandardJournalReader(journal)) {
                            final ProvenanceEventRecord record = reader.nextEvent();
                            this.earliestEventTime = record.getEventTime();
                            break;
                        } catch (final IOException ioe) {
                        }
                    }
                    
                    // Whatever was the last journal for this partition, we need to remove anything for that journal
                    // from the index and re-add them, and then sync the index. This allows us to avoid syncing
                    // the index each time (we sync only on rollover) but allows us to still ensure that we index
                    // all events.
                    if ( latestJournal != null ) {
                        try {
                            reindex(latestJournal);
                        } catch (final EOFException eof) {
                        }
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    
    private void reindex(final File journalFile) throws IOException {
        // TODO: Rework how recovery works because we now have index manager!!
        try (final TocJournalReader reader = new TocJournalReader(containerName, String.valueOf(sectionIndex), String.valueOf(getJournalId(journalFile)), journalFile)) {
            // We don't know which index contains the data for this journal, so remove the journal
            // from both.
            for (final LuceneIndexWriter indexWriter : indexWriters ) {
                indexWriter.delete(containerName, String.valueOf(sectionIndex), String.valueOf(getJournalId(journalFile)));
            }
            
            long maxId = -1L;
            final List<JournaledProvenanceEvent> storedEvents = new ArrayList<>(1000);
            JournaledProvenanceEvent event;
            final LuceneIndexWriter indexWriter = indexWriters[0];
            while ((event = reader.nextJournaledEvent()) != null ) {
                storedEvents.add(event);
                maxId = event.getEventId();
                
                if ( storedEvents.size() == 1000 ) {
                    indexWriter.index(storedEvents);
                    storedEvents.clear();
                }
            }

            if ( !storedEvents.isEmpty() ) {
                indexWriter.index(storedEvents);
            }
            
            indexWriter.sync();
            this.maxEventId = maxId;
        }
    }

    
    @Override
    public List<JournaledStorageLocation> getEvents(final long minEventId, final int maxRecords) throws IOException {
        try (final EventIndexSearcher searcher = newIndexSearcher()) {
            return searcher.getEvents(minEventId, maxRecords);
        }
    }
    
    @Override
    public void shutdown() {
        if ( journalWriter != null ) {
            try {
                journalWriter.finishBlock();
            } catch (final IOException ioe) {
                logger.warn("Failed to finish writing Block to {} due to {}", journalWriter, ioe);
                if ( logger.isDebugEnabled() ) {
                    logger.warn("", ioe);
                }
            }
            
            try {
                journalWriter.close();
            } catch (final IOException ioe) {
                logger.warn("Failed to close {} due to {}", journalWriter, ioe);
                if ( logger.isDebugEnabled() ) {
                    logger.warn("", ioe);
                }
            }
            
            try {
                tocWriter.close();
            } catch (final IOException ioe) {
                logger.warn("Failed to close {} due to {}", tocWriter, ioe);
                if ( logger.isDebugEnabled() ) {
                    logger.warn("", ioe);
                }
            }
        }
        
    }
    
    @Override
    public long getMaxEventId() {
        return maxEventId;
    }

    @Override
    public Long getEarliestEventTime() throws IOException {
        return earliestEventTime;
    }
    
    @Override
    public String toString() {
        return "Partition[section=" + sectionIndex + "]";
    }
}
