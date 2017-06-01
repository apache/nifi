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

package org.apache.nifi.processors.standard.merge;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.standard.MergeRecord;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.stream.io.ByteCountingOutputStream;

public class RecordBin {
    public static final String MERGE_COUNT_ATTRIBUTE = "merge.count";
    public static final String MERGE_BIN_AGE_ATTRIBUTE = "merge.bin.age";

    private final ComponentLog logger;
    private final ProcessSession session;
    private final RecordSetWriterFactory writerFactory;
    private final RecordBinThresholds thresholds;
    private final ProcessContext context;

    private final List<FlowFile> flowFiles = new ArrayList<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();
    private final long creationNanos = System.nanoTime();

    private FlowFile merged;
    private RecordSetWriter recordWriter;
    private ByteCountingOutputStream out;
    private int recordCount = 0;
    private volatile boolean complete = false;

    private static final AtomicLong idGenerator = new AtomicLong(0L);
    private final long id = idGenerator.getAndIncrement();


    public RecordBin(final ProcessContext context, final ProcessSession session, final ComponentLog logger, final RecordBinThresholds thresholds) {
        this.session = session;
        this.writerFactory = context.getProperty(MergeRecord.RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        this.logger = logger;
        this.context = context;

        this.merged = session.create();
        this.thresholds = thresholds;
    }

    public boolean isOlderThan(final RecordBin other) {
        return creationNanos < other.creationNanos;
    }

    public boolean isOlderThan(final long period, final TimeUnit unit) {
        final long nanos = unit.toNanos(period);
        return creationNanos < System.nanoTime() - nanos;
    }

    public boolean isComplete() {
        return complete;
    }

    public boolean offer(final FlowFile flowFile, final RecordReader recordReader, final ProcessSession flowFileSession, final boolean block)
        throws IOException, MalformedRecordException, SchemaNotFoundException {

        if (isComplete()) {
            logger.debug("RecordBin.offer for id={} returning false because {} is complete", new Object[] {flowFile.getId(), this});
            return false;
        }

        final boolean locked;
        if (block) {
            writeLock.lock();
            locked = true;
        } else {
            locked = writeLock.tryLock();
        }

        if (!locked) {
            logger.debug("RecordBin.offer for id={} returning false because failed to get lock for {}", new Object[] {flowFile.getId(), this});
            return false;
        }

        boolean flowFileMigrated = false;

        try {
            if (isComplete()) {
                logger.debug("RecordBin.offer for id={} returning false because {} is complete", new Object[] {flowFile.getId(), this});
                return false;
            }

            logger.debug("Migrating id={} to {}", new Object[] {flowFile.getId(), this});

            Record record;
            while ((record = recordReader.nextRecord()) != null) {
                if (recordWriter == null) {
                    final OutputStream rawOut = session.write(merged);
                    logger.debug("Created OutputStream using session {} for {}", new Object[] {session, this});

                    this.out = new ByteCountingOutputStream(rawOut);

                    recordWriter = writerFactory.createWriter(logger, record.getSchema(), out);
                    recordWriter.beginRecordSet();
                }

                recordWriter.write(record);
                recordCount++;
            }

            // This will be closed by the MergeRecord class anyway but we have to close it
            // here because it needs to be closed before we are able to migrate the FlowFile
            // to a new Session.
            recordReader.close();
            flowFileSession.migrate(this.session, Collections.singleton(flowFile));
            flowFileMigrated = true;
            this.flowFiles.add(flowFile);

            if (isFull()) {
                logger.debug(this + " is now full. Completing bin.");
                complete("Bin is full");
            } else if (isOlderThan(thresholds.getMaxBinMillis(), TimeUnit.MILLISECONDS)) {
                logger.debug(this + " is now expired. Completing bin.");
                complete("Bin is older than " + thresholds.getMaxBinAge());
            }

            return true;
        } catch (final Exception e) {
            logger.error("Failed to create merged FlowFile from " + (flowFiles.size() + 1) + " input FlowFiles; routing originals to failure", e);

            try {
                // This will be closed by the MergeRecord class anyway but we have to close it
                // here because it needs to be closed before we are able to migrate the FlowFile
                // to a new Session.
                recordReader.close();

                if (recordWriter != null) {
                    recordWriter.close();
                }
                if (this.out != null) {
                    this.out.close();
                }

                if (!flowFileMigrated) {
                    flowFileSession.migrate(this.session, Collections.singleton(flowFile));
                    this.flowFiles.add(flowFile);
                }
            } finally {
                complete = true;
                session.remove(merged);
                session.transfer(flowFiles, MergeRecord.REL_FAILURE);
                session.commit();
            }

            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean isFull() {
        readLock.lock();
        try {
            if (!isFullEnough()) {
                return false;
            }

            int maxRecords;
            final Optional<String> recordCountAttribute = thresholds.getRecordCountAttribute();
            if (recordCountAttribute.isPresent()) {
                final Optional<String> recordCountValue = flowFiles.stream()
                    .filter(ff -> ff.getAttribute(recordCountAttribute.get()) != null)
                    .map(ff -> ff.getAttribute(recordCountAttribute.get()))
                    .findFirst();

                if (!recordCountValue.isPresent()) {
                    return false;
                }

                try {
                    maxRecords = Integer.parseInt(recordCountValue.get());
                } catch (final NumberFormatException e) {
                    maxRecords = 1;
                }
            } else {
                maxRecords = thresholds.getMaxRecords();
            }

            if (recordCount >= maxRecords) {
                return true;
            }

            if (out.getBytesWritten() >= thresholds.getMaxBytes()) {
                return true;
            }

            return false;
        } finally {
            readLock.unlock();
        }
    }

    public boolean isFullEnough() {
        readLock.lock();
        try {
            if (flowFiles.isEmpty()) {
                return false;
            }

            int requiredRecordCount;
            final Optional<String> recordCountAttribute = thresholds.getRecordCountAttribute();
            if (recordCountAttribute.isPresent()) {
                final String recordCountValue = flowFiles.get(0).getAttribute(recordCountAttribute.get());
                try {
                    requiredRecordCount = Integer.parseInt(recordCountValue);
                } catch (final NumberFormatException e) {
                    requiredRecordCount = 1;
                }
            } else {
                requiredRecordCount = thresholds.getMinRecords();
            }

            return (recordCount >= requiredRecordCount && out.getBytesWritten() >= thresholds.getMinBytes());
        } finally {
            readLock.unlock();
        }
    }


    public void rollback() {
        complete = true;
        logger.debug("Marked {} as complete because rollback() was called", new Object[] {this});

        writeLock.lock();
        try {
            if (recordWriter != null) {
                try {
                    recordWriter.close();
                } catch (IOException e) {
                    logger.warn("Failed to close Record Writer", e);
                }
            }

            session.rollback();

            if (logger.isDebugEnabled()) {
                final List<String> ids = flowFiles.stream().map(ff -> " id=" + ff.getId() + ",").collect(Collectors.toList());
                logger.debug("Rolled back bin {} containing input FlowFiles {}", new Object[] {this, ids});
            }
        } finally {
            writeLock.unlock();
        }
    }

    private long getBinAge() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - creationNanos);
    }

    private void fail() {
        complete = true;
        logger.debug("Marked {} as complete because fail() was called", new Object[] {this});

        writeLock.lock();
        try {
            if (recordWriter != null) {
                try {
                    recordWriter.close();
                } catch (IOException e) {
                    logger.warn("Failed to close Record Writer", e);
                }
            }

            session.remove(merged);
            session.transfer(flowFiles, MergeRecord.REL_FAILURE);
            session.commit();
        } finally {
            writeLock.unlock();
        }
    }

    public void complete(final String completionReason) throws IOException {
        writeLock.lock();
        try {
            if (isComplete()) {
                logger.debug("Cannot complete {} because it is already completed", new Object[] {this});
                return;
            }

            complete = true;
            logger.debug("Marked {} as complete because complete() was called", new Object[] {this});

            final WriteResult writeResult = recordWriter.finishRecordSet();
            recordWriter.close();
            logger.debug("Closed Record Writer using session {} for {}", new Object[] {session, this});

            if (flowFiles.isEmpty()) {
                session.remove(merged);
                return;
            }

            // If using defragment mode, and we don't have enough FlowFiles, then we need to fail this bin.
            final Optional<String> countAttr = thresholds.getRecordCountAttribute();
            if (countAttr.isPresent()) {
                // Ensure that at least one FlowFile has a fragment.count attribute and that they all have the same value, if they have a value.
                Integer expectedBinCount = null;
                for (final FlowFile flowFile : flowFiles) {
                    final String countVal = flowFile.getAttribute(countAttr.get());
                    if (countVal == null) {
                        continue;
                    }

                    final int count;
                    try {
                        count = Integer.parseInt(countVal);
                    } catch (final NumberFormatException nfe) {
                        logger.error("Could not merge bin with {} FlowFiles because the '{}' attribute had a value of '{}' for {} but expected a number",
                            new Object[] {flowFiles.size(), countAttr.get(), countVal, flowFile});
                        fail();
                        return;
                    }

                    if (expectedBinCount != null && count != expectedBinCount) {
                        logger.error("Could not merge bin with {} FlowFiles because the '{}' attribute had a value of '{}' for {} but another FlowFile in the bin had a value of {}",
                            new Object[] {flowFiles.size(), countAttr.get(), countVal, flowFile, expectedBinCount});
                        fail();
                        return;
                    }

                    expectedBinCount = count;
                }

                if (expectedBinCount == null) {
                    logger.error("Could not merge bin with {} FlowFiles because the '{}' attribute was not present on any of the FlowFiles",
                        new Object[] {flowFiles.size(), countAttr.get()});
                    fail();
                    return;
                }

                if (expectedBinCount != flowFiles.size()) {
                    logger.error("Could not merge bin with {} FlowFiles because the '{}' attribute had a value of '{}' but only {} of {} FlowFiles were encountered before this bin was evicted "
                        + "(due to to Max Bin Age being reached or due to the Maximum Number of Bins being exceeded).",
                        new Object[] {flowFiles.size(), countAttr.get(), expectedBinCount, flowFiles.size(), expectedBinCount});
                    fail();
                    return;
                }
            }

            final Map<String, String> attributes = new HashMap<>();

            final AttributeStrategy attributeStrategy = AttributeStrategyUtil.strategyFor(context);
            final Map<String, String> mergedAttributes = attributeStrategy.getMergedAttributes(flowFiles);
            attributes.putAll(mergedAttributes);

            attributes.putAll(writeResult.getAttributes());
            attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), recordWriter.getMimeType());
            attributes.put(MERGE_COUNT_ATTRIBUTE, Integer.toString(flowFiles.size()));
            attributes.put(MERGE_BIN_AGE_ATTRIBUTE, Long.toString(getBinAge()));

            merged = session.putAllAttributes(merged, attributes);

            session.getProvenanceReporter().join(flowFiles, merged, "Records Merged due to: " + completionReason);
            session.transfer(merged, MergeRecord.REL_MERGED);
            session.transfer(flowFiles, MergeRecord.REL_ORIGINAL);
            session.adjustCounter("Records Merged", writeResult.getRecordCount(), false);
            session.commit();

            if (logger.isDebugEnabled()) {
                final List<String> ids = flowFiles.stream().map(ff -> "id=" + ff.getId()).collect(Collectors.toList());
                logger.debug("Completed bin {} with {} records with Merged FlowFile {} using input FlowFiles {}", new Object[] {this, writeResult.getRecordCount(), merged, ids});
            }
        } catch (final Exception e) {
            session.rollback(true);
            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String toString() {
        readLock.lock();
        try {
            return "RecordBin[size=" + flowFiles.size() + ", full=" + isFull() + ", isComplete=" + isComplete() + ", id=" + id + "]";
        } finally {
            readLock.unlock();
        }
    }
}
