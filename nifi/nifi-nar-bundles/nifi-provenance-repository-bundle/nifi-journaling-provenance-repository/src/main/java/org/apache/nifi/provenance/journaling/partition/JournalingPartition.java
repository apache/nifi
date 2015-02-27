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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.journaling.JournaledProvenanceEvent;
import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.journaling.index.EventIndexSearcher;
import org.apache.nifi.provenance.journaling.index.EventIndexWriter;
import org.apache.nifi.provenance.journaling.index.IndexManager;
import org.apache.nifi.provenance.journaling.index.QueryUtils;
import org.apache.nifi.provenance.journaling.io.StandardEventSerializer;
import org.apache.nifi.provenance.journaling.journals.JournalReader;
import org.apache.nifi.provenance.journaling.journals.JournalWriter;
import org.apache.nifi.provenance.journaling.journals.StandardJournalReader;
import org.apache.nifi.provenance.journaling.journals.StandardJournalWriter;
import org.apache.nifi.provenance.journaling.tasks.CompressionTask;
import org.apache.nifi.provenance.journaling.toc.StandardTocReader;
import org.apache.nifi.provenance.journaling.toc.StandardTocWriter;
import org.apache.nifi.provenance.journaling.toc.TocReader;
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
    private final IndexManager indexManager;
    private final AtomicLong containerSize;
    private final ExecutorService executor;
    private final JournalingRepositoryConfig config;
    
    private JournalWriter journalWriter;
    private TocWriter tocWriter;
    private int numEventsAtEndOfLastBlock = 0;
    private volatile long maxEventId = -1L;
    private volatile Long earliestEventTime = null;

    private final Lock lock = new ReentrantLock();
    private boolean writable = true;    // guarded by lock
    private final List<File> timeOrderedJournalFiles = Collections.synchronizedList(new ArrayList<File>());
    private final AtomicLong partitionSize = new AtomicLong(0L);
    
    public JournalingPartition(final IndexManager indexManager, final String containerName, final int sectionIndex, final File sectionDir, 
            final JournalingRepositoryConfig config, final AtomicLong containerSize, final ExecutorService compressionExecutor) throws IOException {
        this.indexManager = indexManager;
        this.containerSize = containerSize;
        this.containerName = containerName;
        this.sectionIndex = sectionIndex;
        this.section = sectionDir;
        this.journalsDir = new File(section, "journals");
        this.config = config;
        this.executor = compressionExecutor;
        
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
        if ( events.isEmpty() ) {
            return Collections.emptyList();
        }
        
        lock.lock();
        try {
            if ( !writable ) {
                throw new IOException("Cannot write to partition " + this + " because there was previously a write failure. The partition will fix itself in time if I/O problems are resolved");
            }
            
            final JournalWriter writer = getJournalWriter(firstEventId);
    
            final int eventsWritten = writer.getEventCount();
            if ( eventsWritten - numEventsAtEndOfLastBlock > config.getBlockSize() ) {
                writer.finishBlock();
                tocWriter.addBlockOffset(writer.getSize());
                numEventsAtEndOfLastBlock = eventsWritten;
                writer.beginNewBlock();
            }
    
            writer.write(events, firstEventId);
            
            final List<JournaledProvenanceEvent> storedEvents = new ArrayList<>(events.size());
            long id = firstEventId;
            for (final ProvenanceEventRecord event : events) {
                final JournaledStorageLocation location = new JournaledStorageLocation(containerName, String.valueOf(sectionIndex), 
                        writer.getJournalId(), tocWriter.getCurrentBlockIndex(), id++);
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
        } catch (final IOException ioe) {
            writable = false;
            throw ioe;
        } finally {
            lock.unlock();
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
    
    private void updateSize(final long delta) {
        partitionSize.addAndGet(delta);
        containerSize.addAndGet(delta);
    }
    
    // MUST be called with write lock held.
    /**
     * Rolls over the current journal (if any) and begins writing top a new journal.
     * 
     * <p>
     * <b>NOTE:</b> This method MUST be called with the write lock held!!
     * </p>
     * 
     * @param firstEventId the ID of the first event to add to this journal
     * @throws IOException
     */
    private void rollover(final long firstEventId) throws IOException {
        // if we have a writer already, close it and initiate rollover actions
        final File finishedFile = journalWriter == null ? null : journalWriter.getJournalFile();
        if ( journalWriter != null ) {
            journalWriter.finishBlock();
            journalWriter.close();
            tocWriter.close();

            final File finishedTocFile = tocWriter.getFile();
            updateSize(finishedFile.length());
            
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    if ( config.isCompressOnRollover() ) {
                        final long originalSize = finishedFile.length();
                        final long compressedFileSize = new CompressionTask(finishedFile, journalWriter.getJournalId(), finishedTocFile).call();
                        final long sizeAdded = compressedFileSize - originalSize;
                        updateSize(sizeAdded);
                    }
                }
            });
            
            timeOrderedJournalFiles.add(finishedFile);
        }
        
        // create new writers and reset state.
        final File journalFile = new File(journalsDir, firstEventId + JOURNAL_FILE_EXTENSION);
        journalWriter = new StandardJournalWriter(firstEventId, journalFile, false, new StandardEventSerializer());
        try {
            tocWriter = new StandardTocWriter(QueryUtils.getTocFile(journalFile), false, config.isAlwaysSync());
            tocWriter.addBlockOffset(journalWriter.getSize());
            numEventsAtEndOfLastBlock = 0;
        } catch (final Exception e) {
            try {
                journalWriter.close();
            } catch (final IOException ioe) {}
            
            journalWriter = null;
            
            throw e;
        }
        
        logger.debug("Rolling over {} from {} to {}", this, finishedFile, journalFile);
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
        lock.lock();
        try {
            // delete or rename files if stopped during rollover; compress any files that haven't been compressed
            if ( !config.isReadOnly() ) {
                final File[] children = journalsDir.listFiles();
                if ( children != null ) {
                    final List<File> journalFiles = new ArrayList<>();
                    
                    // find any journal files that either haven't been compressed or were partially compressed when
                    // we last shutdown and then restart compression.
                    for ( final File file : children ) {
                        final String filename = file.getName();
                        if ( !filename.endsWith(JOURNAL_FILE_EXTENSION) ) {
                            continue;
                        }
                        
                        journalFiles.add(file);
                        updateSize(file.length());
                        
                        if ( !config.isCompressOnRollover() ) {
                            continue;
                        }
                        
                        if ( filename.endsWith(CompressionTask.FILE_EXTENSION) ) {
                            final File uncompressedFile = new File(journalsDir, filename.replace(CompressionTask.FILE_EXTENSION, ""));
                            if ( uncompressedFile.exists() ) {
                                // both the compressed and uncompressed version of this journal exist. The Compression Task was
                                // not complete when we shutdown. Delete the compressed journal and toc and re-start the Compression Task.
                                final File tocFile = QueryUtils.getTocFile(uncompressedFile);
                                executor.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        final long originalSize = uncompressedFile.length();
                                        final long compressedSize = new CompressionTask(uncompressedFile, getJournalId(uncompressedFile), tocFile).call();
                                        final long sizeAdded = compressedSize - originalSize;
                                        updateSize(sizeAdded);
                                    }
                                });
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
                    
                    // we want to sort the list of all journal files.
                    // we need to create a map of file to last mod time, rather than comparing
                    // by using File.lastModified() because the File.lastModified() value could potentially
                    // change while running the comparator, which violates the comparator's contract.
                    timeOrderedJournalFiles.addAll(journalFiles);
                    final Map<File, Long> lastModTimes = new HashMap<>();
                    for ( final File journalFile : journalFiles ) {
                        lastModTimes.put(journalFile, journalFile.lastModified());
                    }
                    Collections.sort(timeOrderedJournalFiles, new Comparator<File>() {
                        @Override
                        public int compare(final File o1, final File o2) {
                            return lastModTimes.get(o1).compareTo(lastModTimes.get(o2));
                        }
                    });
                    
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
                            if ( record != null ) {
                                this.earliestEventTime = record.getEventTime();
                                break;
                            }
                        } catch (final IOException ioe) {
                        }
                    }

                    // order such that latest journal file is first.
                    Collections.reverse(journalFiles);
                    for ( final File journal : journalFiles ) {
                        try (final JournalReader reader = new StandardJournalReader(journal);
                             final TocReader tocReader = new StandardTocReader(QueryUtils.getTocFile(journal))) {
                            
                            final long lastBlockOffset = tocReader.getLastBlockOffset();
                            final ProvenanceEventRecord lastEvent = reader.getLastEvent(lastBlockOffset);
                            if ( lastEvent != null ) {
                                maxEventId = lastEvent.getEventId() + 1;
                                break;
                            }
                        } catch (final EOFException eof) {}
                    }

                    // We need to re-index all of the journal files that have not been indexed. We can do this by determining
                    // what is the largest event id that has been indexed for this container and section, and then re-indexing
                    // any file that has an event with an id larger than that.
                    // In order to do that, we iterate over the journal files in the order of newest (largest id) to oldest
                    // (smallest id). If the first event id in a file is greater than the max indexed, we re-index the file.
                    // Beyond that, we need to re-index one additional journal file because it's possible that if the first id
                    // is 10 and the max index id is 15, the file containing 10 could also go up to 20. So we re-index one
                    // file that has a min id less than what has been indexed; then we are done.
                    final Long maxIndexedId = indexManager.getMaxEventId(containerName, String.valueOf(sectionIndex));
                    final List<File> reindexJournals = new ArrayList<>();
                    for ( final File journalFile : journalFiles ) {
                        final Long firstEventId;
                        try {
                            firstEventId = getJournalId(journalFile);
                        } catch (final NumberFormatException nfe) {
                            // not a journal; skip this file
                            continue;
                        }
                        
                        if ( maxIndexedId == null || firstEventId > maxIndexedId ) {
                            reindexJournals.add(journalFile);
                        } else {
                            reindexJournals.add(journalFile);
                            break;
                        }
                    }
                    
                    // Make sure that the indexes are not pointing to events that no longer exist.
                    if ( journalFiles.isEmpty() ) {
                        indexManager.deleteEventsBefore(containerName, sectionIndex, Long.MAX_VALUE);
                    } else {
                        final File firstJournalFile = journalFiles.get(0);
                        indexManager.deleteEventsBefore(containerName, sectionIndex, getJournalId(firstJournalFile));
                    }
                    
                    // The reindexJournals list is currently in order of newest to oldest. We need to re-index
                    // in order of oldest to newest, so reverse the list.
                    Collections.reverse(reindexJournals);
                    
                    logger.info("Reindexing {} journal files that were not found in index for container {} and section {}", reindexJournals.size(), containerName, sectionIndex);
                    final long reindexStart = System.nanoTime();
                    for ( final File journalFile : reindexJournals ) {
                        indexManager.reindex(containerName, sectionIndex, getJournalId(journalFile), journalFile);
                    }
                    final long reindexMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - reindexStart);
                    logger.info("Finished reindexing {} journal files for container {} and section {}; reindex took {} millis", 
                            reindexJournals.size(), containerName, sectionIndex, reindexMillis);
                }
            }
        } finally {
            lock.unlock();
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
    
    @Override
    public void verifyWritable(final long nextId) throws IOException {
        final long freeSpace = section.getFreeSpace();
        final long freeMegs = freeSpace / 1024 / 1024;
        if (freeMegs < 10) {
            // if not at least 10 MB, don't even try to write
            throw new IOException("Not Enough Disk Space: partition housing " + section + " has only " + freeMegs + " MB of storage available");
        }
        
        rollover(nextId);
        writable = true;
    }
    
    private boolean delete(final File journalFile) {
        for (int i=0; i < 10; i++) {
            if ( journalFile.delete() || !journalFile.exists() ) {
                return true;
            } else {
                try {
                    Thread.sleep(100L);
                } catch (final InterruptedException ie) {}
            }
        }
        
        return false;
    }
    
    @Override
    public void deleteOldEvents(final long earliestEventTimeToDelete) throws IOException {
        final Set<File> removeFromTimeOrdered = new HashSet<>();
        
        final long start = System.nanoTime();
        try {
            for ( final File journalFile : timeOrderedJournalFiles ) {
                // since these are time-ordered, if we find one that we don't want to delete, we're done.
                if ( journalFile.lastModified() < earliestEventTimeToDelete ) {
                    return;
                }
                
                final long journalSize;
                if ( journalFile.exists() ) {
                    journalSize = journalFile.length();
                } else {
                    continue;
                }
                
                if ( delete(journalFile) ) {
                    removeFromTimeOrdered.add(journalFile);
                } else {
                    logger.warn("Failed to remove expired journal file {}; will attempt to delete again later", journalFile);
                }
                
                updateSize(-journalSize);
                final File tocFile = QueryUtils.getTocFile(journalFile);
                if ( !delete(tocFile) ) {
                    logger.warn("Failed to remove TOC file for expired journal file {}; will attempt to delete again later", journalFile);
                }
            }
        } finally {
            timeOrderedJournalFiles.removeAll(removeFromTimeOrdered);
        }
        
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.info("Removed {} expired journal files from container {}, section {}; total time for deletion was {} millis", 
                removeFromTimeOrdered.size(), containerName, sectionIndex, millis);
    }
    
    
    @Override
    public void deleteOldest() throws IOException {
        File removeFromTimeOrdered = null;
        
        final long start = System.nanoTime();
        try {
            for ( final File journalFile : timeOrderedJournalFiles ) {
                final long journalSize;
                if ( journalFile.exists() ) {
                    journalSize = journalFile.length();
                } else {
                    continue;
                }
                
                if ( delete(journalFile) ) {
                    removeFromTimeOrdered = journalFile;
                } else {
                    throw new IOException("Cannot delete oldest event file " + journalFile);
                }
                
                final File tocFile = QueryUtils.getTocFile(journalFile);
                if ( !delete(tocFile) ) {
                    logger.warn("Failed to remove TOC file for expired journal file {}; will attempt to delete again later", journalFile);
                }
                
                updateSize(-journalSize);
                indexManager.deleteEvents(containerName, sectionIndex, getJournalId(journalFile));
            }
        } finally {
            if ( removeFromTimeOrdered != null ) {
                timeOrderedJournalFiles.remove(removeFromTimeOrdered);
                
                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                logger.info("Removed oldest event file {} from container {}, section {}; total time for deletion was {} millis", 
                        removeFromTimeOrdered, containerName, sectionIndex, millis);
            } else {
                logger.debug("No journals to remove for {}", this);
            }
        }
    }
    
    
    @Override
    public long getPartitionSize() {
        return partitionSize.get();
    }
    
    @Override
    public long getContainerSize() {
        return containerSize.get();
    }
    
    @Override
    public String getContainerName() {
        return containerName;
    }
}
