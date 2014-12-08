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
package org.apache.nifi.controller;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.nifi.controller.repository.ConnectionSwapInfo;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.QueueProvider;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ContentClaimManager;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.io.BufferedOutputStream;
import org.apache.nifi.processor.QueueSize;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * An implementation of the {@link FlowFileSwapManager} that swaps FlowFiles
 * to/from local disk
 * </p>
 */
public class FileSystemSwapManager implements FlowFileSwapManager {

    public static final int MINIMUM_SWAP_COUNT = 10000;
    private static final Pattern SWAP_FILE_PATTERN = Pattern.compile("\\d+-.+\\.swap");
    public static final int SWAP_ENCODING_VERSION = 6;

    private final ScheduledExecutorService swapQueueIdentifierExecutor;
    private final ScheduledExecutorService swapInExecutor;
    private volatile FlowFileRepository flowFileRepository;

    // Maintains a mapping of FlowFile Queue to the a QueueLockWrapper, which provides queue locking and necessary state for swapping back in
    private final ConcurrentMap<FlowFileQueue, QueueLockWrapper> swapMap = new ConcurrentHashMap<>();
    private final File storageDirectory;
    private final long swapInMillis;
    private final long swapOutMillis;
    private final int swapOutThreadCount;

    private ContentClaimManager claimManager;	// effectively final

    private static final Logger logger = LoggerFactory.getLogger(FileSystemSwapManager.class);

    public FileSystemSwapManager() {
        this.storageDirectory = NiFiProperties.getInstance().getSwapStorageLocation();
        if (!storageDirectory.exists() && !storageDirectory.mkdirs()) {
            throw new RuntimeException("Cannot create Swap Storage directory " + storageDirectory.getAbsolutePath());
        }

        swapQueueIdentifierExecutor = new FlowEngine(1, "Identifies Queues for FlowFile Swapping");

        final NiFiProperties properties = NiFiProperties.getInstance();
        swapInMillis = FormatUtils.getTimeDuration(properties.getSwapInPeriod(), TimeUnit.MILLISECONDS);
        swapOutMillis = FormatUtils.getTimeDuration(properties.getSwapOutPeriod(), TimeUnit.MILLISECONDS);
        swapOutThreadCount = properties.getSwapOutThreads();
        swapInExecutor = new FlowEngine(properties.getSwapInThreads(), "Swap In FlowFiles");
    }

    @Override
    public void purge() {
        final File[] swapFiles = storageDirectory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return SWAP_FILE_PATTERN.matcher(name).matches();
            }
        });

        if (swapFiles != null) {
            for (final File file : swapFiles) {
                if (!file.delete() && file.exists()) {
                    logger.warn("Failed to delete SWAP file {}", file);
                }
            }
        }
    }

    public synchronized void start(final FlowFileRepository flowFileRepository, final QueueProvider connectionProvider, final ContentClaimManager claimManager) {
        this.claimManager = claimManager;
        this.flowFileRepository = flowFileRepository;
        swapQueueIdentifierExecutor.scheduleWithFixedDelay(new QueueIdentifier(connectionProvider), swapOutMillis, swapOutMillis, TimeUnit.MILLISECONDS);
        swapInExecutor.scheduleWithFixedDelay(new SwapInTask(), swapInMillis, swapInMillis, TimeUnit.MILLISECONDS);
    }

    public int serializeFlowFiles(final List<FlowFileRecord> toSwap, final FlowFileQueue queue, final String swapLocation, final OutputStream destination) throws IOException {
        if (toSwap == null || toSwap.isEmpty()) {
            return 0;
        }

        long contentSize = 0L;
        for (final FlowFileRecord record : toSwap) {
            contentSize += record.getSize();
        }

        // persist record to disk via the swap file
        final OutputStream bufferedOut = new BufferedOutputStream(destination);
        final DataOutputStream out = new DataOutputStream(bufferedOut);
        try {
            out.writeInt(SWAP_ENCODING_VERSION);
            out.writeUTF(queue.getIdentifier());
            out.writeInt(toSwap.size());
            out.writeLong(contentSize);

            for (final FlowFileRecord flowFile : toSwap) {
                out.writeLong(flowFile.getId());
                out.writeLong(flowFile.getEntryDate());

                final Set<String> lineageIdentifiers = flowFile.getLineageIdentifiers();
                out.writeInt(lineageIdentifiers.size());
                for (final String lineageId : lineageIdentifiers) {
                    out.writeUTF(lineageId);
                }

                out.writeLong(flowFile.getLineageStartDate());
                out.writeLong(flowFile.getLastQueueDate());
                out.writeLong(flowFile.getSize());

                final ContentClaim claim = flowFile.getContentClaim();
                if (claim == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    out.writeUTF(claim.getId());
                    out.writeUTF(claim.getContainer());
                    out.writeUTF(claim.getSection());
                    out.writeLong(flowFile.getContentClaimOffset());
                    out.writeBoolean(claim.isLossTolerant());
                }

                final Map<String, String> attributes = flowFile.getAttributes();
                out.writeInt(attributes.size());
                for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                    writeString(entry.getKey(), out);
                    writeString(entry.getValue(), out);
                }
            }
        } finally {
            out.flush();
        }

        logger.info("Successfully swapped out {} FlowFiles from {} to Swap File {}", new Object[]{toSwap.size(), queue, swapLocation});

        return toSwap.size();
    }

    private void writeString(final String toWrite, final OutputStream out) throws IOException {
        final byte[] bytes = toWrite.getBytes("UTF-8");
        final int utflen = bytes.length;

        if (utflen < 65535) {
            out.write(utflen >>> 8);
            out.write(utflen);
            out.write(bytes);
        } else {
            out.write(255);
            out.write(255);
            out.write(utflen >>> 24);
            out.write(utflen >>> 16);
            out.write(utflen >>> 8);
            out.write(utflen);
            out.write(bytes);
        }
    }

    static List<FlowFileRecord> deserializeFlowFiles(final DataInputStream in, final FlowFileQueue queue, final ContentClaimManager claimManager) throws IOException {
        final int swapEncodingVersion = in.readInt();
        if (swapEncodingVersion > SWAP_ENCODING_VERSION) {
            throw new IOException("Cannot swap FlowFiles in from SwapFile because the encoding version is "
                    + swapEncodingVersion + ", which is too new (expecting " + SWAP_ENCODING_VERSION + " or less)");
        }

        final String connectionId = in.readUTF();
        if (!connectionId.equals(queue.getIdentifier())) {
            throw new IllegalArgumentException("Cannot restore Swap File because the file indicates that records belong to Connection with ID " + connectionId + " but received Connection " + queue);
        }

        final int numRecords = in.readInt();
        in.readLong();  // Content Size

        return deserializeFlowFiles(in, numRecords, queue, swapEncodingVersion, false, claimManager);
    }

    static List<FlowFileRecord> deserializeFlowFiles(final DataInputStream in, final int numFlowFiles, final FlowFileQueue queue, final int serializationVersion, final boolean incrementContentClaims, final ContentClaimManager claimManager) throws IOException {
        final List<FlowFileRecord> flowFiles = new ArrayList<>();
        for (int i = 0; i < numFlowFiles; i++) {
            // legacy encoding had an "action" because it used to be couple with FlowFile Repository code
            if (serializationVersion < 3) {
                final int action = in.read();
                if (action != 1) {
                    throw new IOException("Swap File is version " + serializationVersion + " but did not contain a 'UPDATE' record type");
                }
            }

            final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
            ffBuilder.id(in.readLong());
            ffBuilder.entryDate(in.readLong());

            if (serializationVersion > 1) {
                // Lineage information was added in version 2
                final int numLineageIdentifiers = in.readInt();
                final Set<String> lineageIdentifiers = new HashSet<>(numLineageIdentifiers);
                for (int lineageIdIdx = 0; lineageIdIdx < numLineageIdentifiers; lineageIdIdx++) {
                    lineageIdentifiers.add(in.readUTF());
                }
                ffBuilder.lineageIdentifiers(lineageIdentifiers);
                ffBuilder.lineageStartDate(in.readLong());

                if (serializationVersion > 5) {
                    ffBuilder.lastQueueDate(in.readLong());
                }
            }

            ffBuilder.size(in.readLong());

            if (serializationVersion < 3) {
                readString(in); // connection Id
            }

            final boolean hasClaim = in.readBoolean();
            if (hasClaim) {
                final String claimId;
                if (serializationVersion < 5) {
                    claimId = String.valueOf(in.readLong());
                } else {
                    claimId = in.readUTF();
                }

                final String container = in.readUTF();
                final String section = in.readUTF();
                final long claimOffset = in.readLong();

                final boolean lossTolerant;
                if (serializationVersion >= 4) {
                    lossTolerant = in.readBoolean();
                } else {
                    lossTolerant = false;
                }

                final ContentClaim claim = claimManager.newContentClaim(container, section, claimId, lossTolerant);

                if (incrementContentClaims) {
                    claimManager.incrementClaimantCount(claim);
                }

                ffBuilder.contentClaim(claim);
                ffBuilder.contentClaimOffset(claimOffset);
            }

            boolean attributesChanged = true;
            if (serializationVersion < 3) {
                attributesChanged = in.readBoolean();
            }

            if (attributesChanged) {
                final int numAttributes = in.readInt();
                for (int j = 0; j < numAttributes; j++) {
                    final String key = readString(in);
                    final String value = readString(in);

                    ffBuilder.addAttribute(key, value);
                }
            }

            final FlowFileRecord record = ffBuilder.build();
            flowFiles.add(record);
        }

        return flowFiles;
    }

    private static String readString(final InputStream in) throws IOException {
        final Integer numBytes = readFieldLength(in);
        if (numBytes == null) {
            throw new EOFException();
        }
        final byte[] bytes = new byte[numBytes];
        fillBuffer(in, bytes, numBytes);
        return new String(bytes, "UTF-8");
    }

    private static Integer readFieldLength(final InputStream in) throws IOException {
        final int firstValue = in.read();
        final int secondValue = in.read();
        if (firstValue < 0) {
            return null;
        }
        if (secondValue < 0) {
            throw new EOFException();
        }
        if (firstValue == 0xff && secondValue == 0xff) {
            int ch1 = in.read();
            int ch2 = in.read();
            int ch3 = in.read();
            int ch4 = in.read();
            if ((ch1 | ch2 | ch3 | ch4) < 0) {
                throw new EOFException();
            }
            return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
        } else {
            return ((firstValue << 8) + (secondValue));
        }
    }

    private static void fillBuffer(final InputStream in, final byte[] buffer, final int length) throws IOException {
        int bytesRead;
        int totalBytesRead = 0;
        while ((bytesRead = in.read(buffer, totalBytesRead, length - totalBytesRead)) > 0) {
            totalBytesRead += bytesRead;
        }
        if (totalBytesRead != length) {
            throw new EOFException();
        }
    }

    private class QueueIdentifier implements Runnable {

        private final QueueProvider connectionProvider;

        public QueueIdentifier(final QueueProvider connectionProvider) {
            this.connectionProvider = connectionProvider;
        }

        @Override
        public void run() {
            final Collection<FlowFileQueue> allQueues = connectionProvider.getAllQueues();
            final BlockingQueue<FlowFileQueue> connectionQueue = new LinkedBlockingQueue<>(allQueues);

            final ThreadFactory threadFactory = new ThreadFactory() {
                @Override
                public Thread newThread(final Runnable r) {
                    final Thread t = new Thread(r);
                    t.setName("Swap Out FlowFiles");
                    return t;
                }
            };

            final ExecutorService workerExecutor = Executors.newFixedThreadPool(swapOutThreadCount, threadFactory);
            for (int i = 0; i < swapOutThreadCount; i++) {
                workerExecutor.submit(new SwapOutTask(connectionQueue));
            }

            workerExecutor.shutdown();

            try {
                workerExecutor.awaitTermination(10, TimeUnit.MINUTES);
            } catch (final InterruptedException e) {
                // oh well...
            }
        }
    }

    private class SwapInTask implements Runnable {

        @Override
        public void run() {
            for (final Map.Entry<FlowFileQueue, QueueLockWrapper> entry : swapMap.entrySet()) {
                final FlowFileQueue flowFileQueue = entry.getKey();

                // if queue is more than 60% of its swap threshold, don't swap flowfiles in
                if (flowFileQueue.unswappedSize() >= ((float) flowFileQueue.getSwapThreshold() * 0.6F)) {
                    continue;
                }

                final QueueLockWrapper queueLockWrapper = entry.getValue();
                if (queueLockWrapper.getLock().tryLock()) {
                    try {
                        final Queue<File> queue = queueLockWrapper.getQueue();

                        // Swap FlowFiles in until we hit 90% of the threshold, or until we're out of files.
                        while (flowFileQueue.unswappedSize() < ((float) flowFileQueue.getSwapThreshold() * 0.9F)) {
                            File swapFile = null;
                            try {
                                swapFile = queue.poll();
                                if (swapFile == null) {
                                    break;
                                }

                                try (final InputStream fis = new FileInputStream(swapFile);
                                        final DataInputStream in = new DataInputStream(fis)) {
                                    final List<FlowFileRecord> swappedFlowFiles = deserializeFlowFiles(in, flowFileQueue, claimManager);
                                    flowFileRepository.swapFlowFilesIn(swapFile.getAbsolutePath(), swappedFlowFiles, flowFileQueue);
                                    flowFileQueue.putSwappedRecords(swappedFlowFiles);
                                }

                                if (!swapFile.delete()) {
                                    logger.warn("Swapped in FlowFiles from file " + swapFile.getAbsolutePath() + " but failed to delete the file; this file can be cleaned up manually");
                                }
                            } catch (final Exception e) {
                                logger.error("Failed to Swap In FlowFiles for {} due to {}", new Object[]{flowFileQueue, e.toString()}, e);
                                if (swapFile != null) {
                                    queue.add(swapFile);
                                }
                            }
                        }
                    } finally {
                        queueLockWrapper.getLock().unlock();
                    }
                }
            }
        }
    }

    private class SwapOutTask implements Runnable {

        private final BlockingQueue<FlowFileQueue> connectionQueue;

        public SwapOutTask(final BlockingQueue<FlowFileQueue> connectionQueue) {
            this.connectionQueue = connectionQueue;
        }

        @Override
        public void run() {
            while (true) {
                final FlowFileQueue flowFileQueue = connectionQueue.poll();
                if (flowFileQueue == null) {
                    logger.debug("No more FlowFile Queues to Swap Out");
                    return;
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("{} has {} FlowFiles to swap out", flowFileQueue, flowFileQueue.getSwapQueueSize());
                }

                while (flowFileQueue.getSwapQueueSize() >= MINIMUM_SWAP_COUNT) {
                    final File swapFile = new File(storageDirectory, System.currentTimeMillis() + "-" + UUID.randomUUID().toString() + ".swap");
                    final String swapLocation = swapFile.getAbsolutePath();
                    final List<FlowFileRecord> toSwap = flowFileQueue.pollSwappableRecords();

                    int recordsSwapped;
                    try (final FileOutputStream fos = new FileOutputStream(swapFile)) {
                        recordsSwapped = serializeFlowFiles(toSwap, flowFileQueue, swapLocation, fos);
                        flowFileRepository.swapFlowFilesOut(toSwap, flowFileQueue, swapLocation);
                        fos.getFD().sync();
                    } catch (final IOException ioe) {
                        recordsSwapped = 0;
                        flowFileQueue.putSwappedRecords(toSwap);
                        logger.error("Failed to swap out {} FlowFiles from {} to Swap File {} due to {}", new Object[]{toSwap.size(), flowFileQueue, swapLocation, ioe.toString()}, ioe);
                    }

                    if (recordsSwapped > 0) {
                        QueueLockWrapper swapQueue = swapMap.get(flowFileQueue);
                        if (swapQueue == null) {
                            swapQueue = new QueueLockWrapper(new LinkedBlockingQueue<File>());
                            QueueLockWrapper oldQueue = swapMap.putIfAbsent(flowFileQueue, swapQueue);
                            if (oldQueue != null) {
                                swapQueue = oldQueue;
                            }
                        }

                        swapQueue.getQueue().add(swapFile);
                    } else {
                        swapFile.delete();
                    }
                }
            }
        }
    }

    /**
     * Recovers FlowFiles from all Swap Files, returning the largest FlowFile ID
     * that was recovered.
     *
     * @param queueProvider
     * @return
     */
    @Override
    public long recoverSwappedFlowFiles(final QueueProvider queueProvider, final ContentClaimManager claimManager) {
        final File[] swapFiles = storageDirectory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return SWAP_FILE_PATTERN.matcher(name).matches();
            }
        });

        if (swapFiles == null) {
            return 0L;
        }

        final Collection<FlowFileQueue> allQueues = queueProvider.getAllQueues();
        final Map<String, FlowFileQueue> queueMap = new HashMap<>();
        for (final FlowFileQueue queue : allQueues) {
            queueMap.put(queue.getIdentifier(), queue);
        }

        final ConnectionSwapInfo swapInfo = new ConnectionSwapInfo();
        int swappedCount = 0;
        long swappedBytes = 0L;
        long maxRecoveredId = 0L;

        for (final File swapFile : swapFiles) {
            // read record to disk via the swap file
            try (final InputStream fis = new FileInputStream(swapFile);
                    final InputStream bufferedIn = new BufferedInputStream(fis);
                    final DataInputStream in = new DataInputStream(bufferedIn)) {

                final int swapEncodingVersion = in.readInt();
                if (swapEncodingVersion > SWAP_ENCODING_VERSION) {
                    throw new IOException("Cannot swap FlowFiles in from " + swapFile + " because the encoding version is "
                            + swapEncodingVersion + ", which is too new (expecting " + SWAP_ENCODING_VERSION + " or less)");
                }

                final String connectionId = in.readUTF();
                final FlowFileQueue queue = queueMap.get(connectionId);
                if (queue == null) {
                    logger.error("Cannot recover Swapped FlowFiles from Swap File {} because the FlowFiles belong to a Connection with ID {} and that Connection does not exist", swapFile, connectionId);
                    continue;
                }

                final int numRecords = in.readInt();
                final long contentSize = in.readLong();

                swapInfo.addSwapSizeInfo(connectionId, swapFile.getAbsolutePath(), new QueueSize(numRecords, contentSize));
                swappedCount += numRecords;
                swappedBytes += contentSize;

                final List<FlowFileRecord> records = deserializeFlowFiles(in, numRecords, queue, swapEncodingVersion, true, claimManager);
                long maxId = 0L;
                for (final FlowFileRecord record : records) {
                    if (record.getId() > maxId) {
                        maxId = record.getId();
                    }
                }

                if (maxId > maxRecoveredId) {
                    maxRecoveredId = maxId;
                }
            } catch (final IOException ioe) {
                logger.error("Cannot recover Swapped FlowFiles from Swap File {} due to {}", swapFile, ioe.toString());
                if (logger.isDebugEnabled()) {
                    logger.error("", ioe);
                }
            }
        }

        restoreSwapLocations(queueMap.values(), swapInfo);
        logger.info("Recovered {} FlowFiles ({} bytes) from Swap Files", swappedCount, swappedBytes);
        return maxRecoveredId;
    }

    public void restoreSwapLocations(final Collection<FlowFileQueue> flowFileQueues, final ConnectionSwapInfo swapInfo) {
        for (final FlowFileQueue queue : flowFileQueues) {
            final String queueId = queue.getIdentifier();
            final Collection<String> swapFileLocations = swapInfo.getSwapFileLocations(queueId);
            if (swapFileLocations == null || swapFileLocations.isEmpty()) {
                continue;
            }

            final SortedMap<String, QueueSize> sortedFileQueueMap = new TreeMap<>(new SwapFileComparator());
            for (final String swapFileLocation : swapFileLocations) {
                final QueueSize queueSize = swapInfo.getSwappedSize(queueId, swapFileLocation);
                sortedFileQueueMap.put(swapFileLocation, queueSize);
            }

            QueueLockWrapper fileQueue = swapMap.get(queue);
            if (fileQueue == null) {
                fileQueue = new QueueLockWrapper(new LinkedBlockingQueue<File>());
                swapMap.put(queue, fileQueue);
            }

            for (final Map.Entry<String, QueueSize> innerEntry : sortedFileQueueMap.entrySet()) {
                final File swapFile = new File(innerEntry.getKey());
                final QueueSize size = innerEntry.getValue();
                fileQueue.getQueue().add(swapFile);
                queue.incrementSwapCount(size.getObjectCount(), size.getByteCount());
            }
        }
    }

    public void shutdown() {
        swapQueueIdentifierExecutor.shutdownNow();
        swapInExecutor.shutdownNow();
    }

    private static class SwapFileComparator implements Comparator<String> {

        @Override
        public int compare(final String o1, final String o2) {
            if (o1 == o2) {
                return 0;
            }

            final Long time1 = getTimestampFromFilename(o1);
            final Long time2 = getTimestampFromFilename(o2);

            if (time1 == null && time2 == null) {
                return 0;
            }
            if (time1 == null) {
                return 1;
            }
            if (time2 == null) {
                return -1;
            }

            final int timeComparisonValue = time1.compareTo(time2);
            if (timeComparisonValue != 0) {
                return timeComparisonValue;
            }

            return o1.compareTo(o2);
        }

        private Long getTimestampFromFilename(final String fullyQualifiedFilename) {
            if (fullyQualifiedFilename == null) {
                return null;
            }

            final File file = new File(fullyQualifiedFilename);
            final String filename = file.getName();

            final int idx = filename.indexOf("-");
            if (idx < 1) {
                return null;
            }

            final String millisVal = filename.substring(0, idx);
            try {
                return Long.parseLong(millisVal);
            } catch (final NumberFormatException e) {
                return null;
            }
        }
    }

    private static class QueueLockWrapper {

        private final Lock lock = new ReentrantLock();
        private final Queue<File> queue;

        public QueueLockWrapper(final Queue<File> queue) {
            this.queue = queue;
        }

        public Queue<File> getQueue() {
            return queue;
        }

        public Lock getLock() {
            return lock;
        }

        @Override
        public int hashCode() {
            return queue.hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof QueueLockWrapper) {
                return queue.equals(((QueueLockWrapper) obj).queue);
            }
            return false;
        }
    }
}
