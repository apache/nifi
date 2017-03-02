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
import java.io.BufferedOutputStream;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.FlowFileSwapManager;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapManagerInitializationContext;
import org.apache.nifi.controller.repository.SwapSummary;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.swap.SchemaSwapDeserializer;
import org.apache.nifi.controller.swap.SchemaSwapSerializer;
import org.apache.nifi.controller.swap.SimpleSwapDeserializer;
import org.apache.nifi.controller.swap.SwapDeserializer;
import org.apache.nifi.controller.swap.SwapSerializer;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.stream.io.StreamUtils;
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

    private static final byte[] MAGIC_HEADER = {'S', 'W', 'A', 'P'};

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

        final SwapSerializer serializer = new SchemaSwapSerializer();
        try (final FileOutputStream fos = new FileOutputStream(swapTempFile);
            final OutputStream out = new BufferedOutputStream(fos)) {
            out.write(MAGIC_HEADER);
            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF(serializer.getSerializationName());

            serializer.serializeFlowFiles(toSwap, flowFileQueue, swapLocation, out);
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

        try (final InputStream fis = new FileInputStream(swapFile);
                final InputStream bis = new BufferedInputStream(fis);
                final DataInputStream in = new DataInputStream(bis)) {

            final SwapDeserializer deserializer = createSwapDeserializer(in);
            return deserializer.deserializeFlowFiles(in, swapLocation, flowFileQueue, claimManager);
        }
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
            if (splits.length > 6) {
                final String queueIdentifier = splits[1] + "-" + splits[2] + "-" + splits[3] + "-" + splits[4] + "-" + splits[5];
                if (queueIdentifier.equals(flowFileQueue.getIdentifier())) {
                    swapLocations.add(swapFile.getAbsolutePath());
                }

                continue;
            }

            // Read the queue identifier from the swap file to check if the swap file is for this queue
            try (final InputStream fis = new FileInputStream(swapFile);
                    final InputStream bufferedIn = new BufferedInputStream(fis);
                    final DataInputStream in = new DataInputStream(bufferedIn)) {

                final SwapDeserializer deserializer;
                try {
                    deserializer = createSwapDeserializer(in);
                } catch (final Exception e) {
                    final String errMsg = "Cannot swap FlowFiles in from " + swapFile + " due to " + e;
                    eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, errMsg);
                    throw new IOException(errMsg);
                }

                // If deserializer is not an instance of Simple Swap Deserializer, then it means that the serializer is new enough that
                // we use the 3-element filename as illustrated above, so this is only necessary for the SimpleSwapDeserializer.
                if (deserializer instanceof SimpleSwapDeserializer) {
                    final String connectionId = in.readUTF();
                    if (connectionId.equals(flowFileQueue.getIdentifier())) {
                        swapLocations.add(swapFile.getAbsolutePath());
                    }
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

            final SwapDeserializer deserializer = createSwapDeserializer(in);
            return deserializer.getSwapSummary(in, swapLocation, claimManager);
        }
    }


    private SwapDeserializer createSwapDeserializer(final DataInputStream dis) throws IOException {
        dis.mark(MAGIC_HEADER.length);

        final byte[] magicHeader = new byte[MAGIC_HEADER.length];
        try {
            StreamUtils.fillBuffer(dis, magicHeader);
        } catch (final EOFException eof) {
            throw new IOException("Failed to read swap file because the file contained less than 4 bytes of data");
        }

        if (Arrays.equals(magicHeader, MAGIC_HEADER)) {
            final String serializationName = dis.readUTF();
            if (serializationName.equals(SchemaSwapDeserializer.getSerializationName())) {
                return new SchemaSwapDeserializer();
            }

            throw new IOException("Cannot find a suitable Deserializer for swap file, written with Serialization Name '" + serializationName + "'");
        } else {
            // SimpleSwapDeserializer is old and did not write out a magic header.
            dis.reset();
            return new SimpleSwapDeserializer();
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
