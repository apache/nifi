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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.IncompleteSwapFileException;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapManagerInitializationContext;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.swap.StandardSwapContents;
import org.apache.nifi.controller.swap.StandardSwapSummary;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.stream.io.BufferedOutputStream;
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
    private static final Pattern TEMP_SWAP_FILE_PATTERN = Pattern.compile("\\d+-.+\\.swap\\.part");

    public static final int SWAP_ENCODING_VERSION = 10;
    public static final String EVENT_CATEGORY = "Swap FlowFiles";
    private static final Logger logger = LoggerFactory.getLogger(FileSystemSwapManager.class);

    private final File storageDirectory;

    // effectively final
    private FlowFileRepository flowFileRepository;
    private EventReporter eventReporter;
    private ResourceClaimManager claimManager;

    /**
     * Default no args constructor for service loading only.
     */
    public FileSystemSwapManager() {
        storageDirectory = null;
    }

    public FileSystemSwapManager(final NiFiProperties nifiProperties) {
        final Path flowFileRepoPath = nifiProperties.getFlowFileRepositoryPath();

        this.storageDirectory = flowFileRepoPath.resolve("swap").toFile();
        if (!storageDirectory.exists() && !storageDirectory.mkdirs()) {
            throw new RuntimeException("Cannot create Swap Storage directory " + storageDirectory.getAbsolutePath());
        }
    }

    @Override
    public synchronized void initialize(final SwapManagerInitializationContext initializationContext) {
        this.claimManager = initializationContext.getResourceClaimManager();
        this.eventReporter = initializationContext.getEventReporter();
        this.flowFileRepository = initializationContext.getFlowFileRepository();
    }

    @Override
    public String swapOut(final List<FlowFileRecord> toSwap, final FlowFileQueue flowFileQueue) throws IOException {
        if (toSwap == null || toSwap.isEmpty()) {
            return null;
        }

        final File swapFile = new File(storageDirectory, System.currentTimeMillis() + "-" + flowFileQueue.getIdentifier() + "-" + UUID.randomUUID().toString() + ".swap");
        final File swapTempFile = new File(swapFile.getParentFile(), swapFile.getName() + ".part");
        final String swapLocation = swapFile.getAbsolutePath();

        try (final FileOutputStream fos = new FileOutputStream(swapTempFile)) {
            serializeFlowFiles(toSwap, flowFileQueue, swapLocation, fos);
            fos.getFD().sync();
        } catch (final IOException ioe) {
            // we failed to write out the entire swap file. Delete the temporary file, if we can.
            swapTempFile.delete();
            throw ioe;
        }

        if (swapTempFile.renameTo(swapFile)) {
            flowFileRepository.swapFlowFilesOut(toSwap, flowFileQueue, swapLocation);
        } else {
            error("Failed to swap out FlowFiles from " + flowFileQueue + " due to: Unable to rename swap file from " + swapTempFile + " to " + swapFile);
        }

        return swapLocation;
    }

    @Override
    public SwapContents swapIn(final String swapLocation, final FlowFileQueue flowFileQueue) throws IOException {
        final File swapFile = new File(swapLocation);
        final SwapContents swapContents = peek(swapLocation, flowFileQueue);
        flowFileRepository.swapFlowFilesIn(swapFile.getAbsolutePath(), swapContents.getFlowFiles(), flowFileQueue);

        if (!swapFile.delete()) {
            warn("Swapped in FlowFiles from file " + swapFile.getAbsolutePath() + " but failed to delete the file; this file should be cleaned up manually");
        }

        return swapContents;
    }

    @Override
    public SwapContents peek(final String swapLocation, final FlowFileQueue flowFileQueue) throws IOException {
        final File swapFile = new File(swapLocation);
        if (!swapFile.exists()) {
            throw new FileNotFoundException("Failed to swap in FlowFiles from external storage location " + swapLocation + " into FlowFile Queue because the file could not be found");
        }

        final SwapContents swapContents;
        try (final InputStream fis = new FileInputStream(swapFile);
                final InputStream bis = new BufferedInputStream(fis);
                final DataInputStream in = new DataInputStream(bis)) {
            swapContents = deserializeFlowFiles(in, swapLocation, flowFileQueue, claimManager);
        }

        return swapContents;
    }

    @Override
    public void purge() {
        final File[] swapFiles = storageDirectory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return SWAP_FILE_PATTERN.matcher(name).matches() || TEMP_SWAP_FILE_PATTERN.matcher(name).matches();
            }
        });

        for (final File file : swapFiles) {
            if (!file.delete()) {
                warn("Failed to delete Swap File " + file + " when purging FlowFile Swap Manager");
            }
        }
    }

    @Override
    public List<String> recoverSwapLocations(final FlowFileQueue flowFileQueue) throws IOException {
        final File[] swapFiles = storageDirectory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return SWAP_FILE_PATTERN.matcher(name).matches() || TEMP_SWAP_FILE_PATTERN.matcher(name).matches();
            }
        });

        if (swapFiles == null) {
            return Collections.emptyList();
        }

        final List<String> swapLocations = new ArrayList<>();
        // remove in .part files, as they are partial swap files that did not get written fully.
        for (final File swapFile : swapFiles) {
            if (TEMP_SWAP_FILE_PATTERN.matcher(swapFile.getName()).matches()) {
                if (swapFile.delete()) {
                    logger.info("Removed incomplete/temporary Swap File " + swapFile);
                } else {
                    warn("Failed to remove incomplete/temporary Swap File " + swapFile + "; this file should be cleaned up manually");
                }

                continue;
            }

            // split the filename by dashes. The old filenaming scheme was "<timestamp>-<randomuuid>.swap" but the new naming scheme is
            // "<timestamp>-<queue identifier>-<random uuid>.swap". If we have two dashes, then we can just check if the queue ID is equal
            // to the id of the queue given and if not we can just move on.
            final String[] splits = swapFile.getName().split("-");
            if (splits.length == 3) {
                final String queueIdentifier = splits[1];
                if (!queueIdentifier.equals(flowFileQueue.getIdentifier())) {
                    continue;
                }
            }

            // Read the queue identifier from the swap file to check if the swap file is for this queue
            try (final InputStream fis = new FileInputStream(swapFile);
                    final InputStream bufferedIn = new BufferedInputStream(fis);
                    final DataInputStream in = new DataInputStream(bufferedIn)) {

                final int swapEncodingVersion = in.readInt();
                if (swapEncodingVersion > SWAP_ENCODING_VERSION) {
                    final String errMsg = "Cannot swap FlowFiles in from " + swapFile + " because the encoding version is "
                            + swapEncodingVersion + ", which is too new (expecting " + SWAP_ENCODING_VERSION + " or less)";

                    eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, errMsg);
                    throw new IOException(errMsg);
                }

                final String connectionId = in.readUTF();
                if (connectionId.equals(flowFileQueue.getIdentifier())) {
                    swapLocations.add(swapFile.getAbsolutePath());
                }
            }
        }

        Collections.sort(swapLocations, new SwapFileComparator());
        return swapLocations;
    }

    @Override
    public SwapSummary getSwapSummary(final String swapLocation) throws IOException {
        final File swapFile = new File(swapLocation);

        // read record from disk via the swap file
        try (final InputStream fis = new FileInputStream(swapFile);
                final InputStream bufferedIn = new BufferedInputStream(fis);
                final DataInputStream in = new DataInputStream(bufferedIn)) {

            final int swapEncodingVersion = in.readInt();
            if (swapEncodingVersion > SWAP_ENCODING_VERSION) {
                final String errMsg = "Cannot swap FlowFiles in from " + swapFile + " because the encoding version is "
                        + swapEncodingVersion + ", which is too new (expecting " + SWAP_ENCODING_VERSION + " or less)";

                eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, errMsg);
                throw new IOException(errMsg);
            }

            final int numRecords;
            final long contentSize;
            Long maxRecordId = null;
            try {
                in.readUTF(); // ignore Connection ID
                numRecords = in.readInt();
                contentSize = in.readLong();

                if (numRecords == 0) {
                    return StandardSwapSummary.EMPTY_SUMMARY;
                }

                if (swapEncodingVersion > 7) {
                    maxRecordId = in.readLong();
                }
            } catch (final EOFException eof) {
                logger.warn("Found premature End-of-File when reading Swap File {}. EOF occurred before any FlowFiles were encountered", swapLocation);
                return StandardSwapSummary.EMPTY_SUMMARY;
            }

            final QueueSize queueSize = new QueueSize(numRecords, contentSize);
            final SwapContents swapContents = deserializeFlowFiles(in, queueSize, maxRecordId, swapEncodingVersion, true, claimManager, swapLocation);
            return swapContents.getSummary();
        }
    }

    public static int serializeFlowFiles(final List<FlowFileRecord> toSwap, final FlowFileQueue queue, final String swapLocation, final OutputStream destination) throws IOException {
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

            // get the max record id and write that out so that we know it quickly for restoration
            long maxRecordId = 0L;
            for (final FlowFileRecord flowFile : toSwap) {
                if (flowFile.getId() > maxRecordId) {
                    maxRecordId = flowFile.getId();
                }
            }

            out.writeLong(maxRecordId);

            for (final FlowFileRecord flowFile : toSwap) {
                out.writeLong(flowFile.getId());
                out.writeLong(flowFile.getEntryDate());
                out.writeLong(flowFile.getLineageStartDate());
                out.writeLong(flowFile.getLineageStartIndex());
                out.writeLong(flowFile.getLastQueueDate());
                out.writeLong(flowFile.getQueueDateIndex());
                out.writeLong(flowFile.getSize());

                final ContentClaim claim = flowFile.getContentClaim();
                if (claim == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    final ResourceClaim resourceClaim = claim.getResourceClaim();
                    out.writeUTF(resourceClaim.getId());
                    out.writeUTF(resourceClaim.getContainer());
                    out.writeUTF(resourceClaim.getSection());
                    out.writeLong(claim.getOffset());
                    out.writeLong(claim.getLength());
                    out.writeLong(flowFile.getContentClaimOffset());
                    out.writeBoolean(resourceClaim.isLossTolerant());
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

        logger.info("Successfully swapped out {} FlowFiles from {} to Swap File {}", toSwap.size(), queue, swapLocation);

        return toSwap.size();
    }

    private static void writeString(final String toWrite, final OutputStream out) throws IOException {
        final byte[] bytes = toWrite.getBytes(StandardCharsets.UTF_8);
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

    static SwapContents deserializeFlowFiles(final DataInputStream in, final String swapLocation, final FlowFileQueue queue, final ResourceClaimManager claimManager) throws IOException {
        final int swapEncodingVersion = in.readInt();
        if (swapEncodingVersion > SWAP_ENCODING_VERSION) {
            throw new IOException("Cannot swap FlowFiles in from SwapFile because the encoding version is "
                    + swapEncodingVersion + ", which is too new (expecting " + SWAP_ENCODING_VERSION + " or less)");
        }

        final String connectionId = in.readUTF(); // Connection ID
        if (!connectionId.equals(queue.getIdentifier())) {
            throw new IllegalArgumentException("Cannot deserialize FlowFiles from Swap File at location " + swapLocation
                    + " because those FlowFiles belong to Connection with ID " + connectionId + " and an attempt was made to swap them into a Connection with ID " + queue.getIdentifier());
        }

        int numRecords = 0;
        long contentSize = 0L;
        Long maxRecordId = null;
        try {
            numRecords = in.readInt();
            contentSize = in.readLong(); // Content Size
            if (swapEncodingVersion > 7) {
                maxRecordId = in.readLong(); // Max Record ID
            }
        } catch (final EOFException eof) {
            final QueueSize queueSize = new QueueSize(numRecords, contentSize);
            final SwapSummary summary = new StandardSwapSummary(queueSize, maxRecordId, Collections.emptyList());
            final SwapContents partialContents = new StandardSwapContents(summary, Collections.emptyList());
            throw new IncompleteSwapFileException(swapLocation, partialContents);
        }

        final QueueSize queueSize = new QueueSize(numRecords, contentSize);
        return deserializeFlowFiles(in, queueSize, maxRecordId, swapEncodingVersion, false, claimManager, swapLocation);
    }

    private static SwapContents deserializeFlowFiles(final DataInputStream in, final QueueSize queueSize, final Long maxRecordId,
            final int serializationVersion, final boolean incrementContentClaims, final ResourceClaimManager claimManager, final String location) throws IOException {
        final List<FlowFileRecord> flowFiles = new ArrayList<>(queueSize.getObjectCount());
        final List<ResourceClaim> resourceClaims = new ArrayList<>(queueSize.getObjectCount());
        Long maxId = maxRecordId;

        for (int i = 0; i < queueSize.getObjectCount(); i++) {
            try {
                // legacy encoding had an "action" because it used to be couple with FlowFile Repository code
                if (serializationVersion < 3) {
                    final int action = in.read();
                    if (action != 1) {
                        throw new IOException("Swap File is version " + serializationVersion + " but did not contain a 'UPDATE' record type");
                    }
                }

                final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
                final long recordId = in.readLong();
                if (maxId == null || recordId > maxId) {
                    maxId = recordId;
                }

                ffBuilder.id(recordId);
                ffBuilder.entryDate(in.readLong());

                if (serializationVersion > 1) {
                    // Lineage information was added in version 2
                    if (serializationVersion < 10) {
                        final int numLineageIdentifiers = in.readInt();
                        for (int lineageIdIdx = 0; lineageIdIdx < numLineageIdentifiers; lineageIdIdx++) {
                            in.readUTF(); //skip each identifier
                        }
                    }

                    // version 9 adds in a 'lineage start index'
                    final long lineageStartDate = in.readLong();
                    final long lineageStartIndex;
                    if (serializationVersion > 8) {
                        lineageStartIndex = in.readLong();
                    } else {
                        lineageStartIndex = 0L;
                    }

                    ffBuilder.lineageStart(lineageStartDate, lineageStartIndex);

                    if (serializationVersion > 5) {
                        // Version 9 adds in a 'queue date index'
                        final long lastQueueDate = in.readLong();
                        final long queueDateIndex;
                        if (serializationVersion > 8) {
                            queueDateIndex = in.readLong();
                        } else {
                            queueDateIndex = 0L;
                        }

                        ffBuilder.lastQueued(lastQueueDate, queueDateIndex);
                    }
                }

                ffBuilder.size(in.readLong());

                if (serializationVersion < 3) {
                    readString(in); // connection Id
                }

                final boolean hasClaim = in.readBoolean();
                ResourceClaim resourceClaim = null;
                if (hasClaim) {
                    final String claimId;
                    if (serializationVersion < 5) {
                        claimId = String.valueOf(in.readLong());
                    } else {
                        claimId = in.readUTF();
                    }

                    final String container = in.readUTF();
                    final String section = in.readUTF();

                    final long resourceOffset;
                    final long resourceLength;
                    if (serializationVersion < 6) {
                        resourceOffset = 0L;
                        resourceLength = -1L;
                    } else {
                        resourceOffset = in.readLong();
                        resourceLength = in.readLong();
                    }

                    final long claimOffset = in.readLong();

                    final boolean lossTolerant;
                    if (serializationVersion >= 4) {
                        lossTolerant = in.readBoolean();
                    } else {
                        lossTolerant = false;
                    }

                    resourceClaim = claimManager.newResourceClaim(container, section, claimId, lossTolerant);
                    final StandardContentClaim claim = new StandardContentClaim(resourceClaim, resourceOffset);
                    claim.setLength(resourceLength);

                    if (incrementContentClaims) {
                        claimManager.incrementClaimantCount(resourceClaim);
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
                if (resourceClaim != null) {
                    resourceClaims.add(resourceClaim);
                }

                flowFiles.add(record);
            } catch (final EOFException eof) {
                final SwapSummary swapSummary = new StandardSwapSummary(queueSize, maxId, resourceClaims);
                final SwapContents partialContents = new StandardSwapContents(swapSummary, flowFiles);
                throw new IncompleteSwapFileException(location, partialContents);
            }
        }

        final SwapSummary swapSummary = new StandardSwapSummary(queueSize, maxId, resourceClaims);
        return new StandardSwapContents(swapSummary, flowFiles);
    }

    private static String readString(final InputStream in) throws IOException {
        final Integer numBytes = readFieldLength(in);
        if (numBytes == null) {
            throw new EOFException();
        }
        final byte[] bytes = new byte[numBytes];
        fillBuffer(in, bytes, numBytes);
        return new String(bytes, StandardCharsets.UTF_8);
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
            final int ch1 = in.read();
            final int ch2 = in.read();
            final int ch3 = in.read();
            final int ch4 = in.read();
            if ((ch1 | ch2 | ch3 | ch4) < 0) {
                throw new EOFException();
            }
            return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4;
        } else {
            return (firstValue << 8) + secondValue;
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

    private void error(final String error) {
        logger.error(error);
        if (eventReporter != null) {
            eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, error);
        }
    }

    private void warn(final String warning) {
        logger.warn(warning);
        if (eventReporter != null) {
            eventReporter.reportEvent(Severity.WARNING, EVENT_CATEGORY, warning);
        }
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
}
