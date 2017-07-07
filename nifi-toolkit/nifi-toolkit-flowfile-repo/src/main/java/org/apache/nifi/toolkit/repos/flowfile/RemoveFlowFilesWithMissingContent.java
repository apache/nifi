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

package org.apache.nifi.toolkit.repos.flowfile;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.nifi.controller.FileSystemSwapManager;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.RepositoryRecordSerde;
import org.apache.nifi.controller.repository.SchemaRepositoryRecordSerde;
import org.apache.nifi.controller.repository.SwapContents;
import org.apache.nifi.controller.repository.SwapManagerInitializationContext;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.swap.SchemaSwapSerializer;
import org.apache.nifi.controller.swap.SwapSerializer;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.util.NiFiProperties;
import org.wali.MinimalLockingWriteAheadLog;
import org.wali.SyncListener;

public class RemoveFlowFilesWithMissingContent {
    private static final byte[] SWAP_MAGIC_HEADER = {'S', 'W', 'A', 'P'};

    private static void printUsage() {
        System.out.println("Makes a copy of an existing FlowFile Repo, inspecting the Content Repo to ensure that all of the FlowFiles' content exists in the Content Repo. "
            + "For any FlowFile whose content is missing, the FlowFile will be written out as a 'DELETED' FlowFile in the new Repo.");
        System.out.println("Usage:");
        System.out.println("java " + RemoveFlowFilesWithMissingContent.class.getCanonicalName() + " <nifi properties file> <destination flowfile repo dir>");
        System.out.println();
        System.out.println("<nifi properties file> : The nifi.properties file that specifies where the flowfile and content repos are");
        System.out.println("<destination flowfile repo dir> : The directory to write the updated FlowFile Repo to");
    }

    public static void main(final String[] args) throws IOException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // Verify Argument Count
        if (args.length != 2) {
            printUsage();
            return;
        }

        final File propsFile = new File(args[0]);
        if (!propsFile.exists()) {
            System.out.println("Cannot find " + propsFile);
            return;
        }

        final NiFiProperties nifiProps = new NiFiPropertiesLoader().load(propsFile);

        // Verify Directories specified
        final File originalDir = new File(nifiProps.getProperty("nifi.flowfile.repository.directory"));
        final Map<String, File> contentDirs = nifiProps.getPropertyKeys().stream()
            .map(key -> key)
            .filter(key -> key.startsWith("nifi.content.repository.directory."))
            .collect(Collectors.toMap(
                key -> substringAfter(key, "nifi.content.repository.directory."),
                key -> new File(nifiProps.getProperty(key))));

        for (final File file : contentDirs.values()) {
            if (!verifyDirectory(file)) {
                return;
            }
        }

        final File destinationDir = new File(args[1]);
        if (destinationDir.exists() && !destinationDir.isDirectory()) {
            System.out.println(destinationDir + " is not a directory");
            return;
        }

        if (!destinationDir.exists() && !destinationDir.mkdirs()) {
            System.out.println("Could not create directory " + destinationDir);
            return;
        }

        if (destinationDir.equals(originalDir)) {
            System.out.println("Destination Directory cannot be the same as the existing FlowFile Repository");
            return;
        }

        System.out.println("Scanning Content Repository to determine which files exists...");
        final ResourceClaimManager claimManager = new StandardResourceClaimManager();
        for (final Map.Entry<String, File> entry : contentDirs.entrySet()) {
            populateClaimManager(entry.getKey(), entry.getValue(), claimManager);
        }

        System.out.println("Finished scanning Content Repository. Loading FlowFiles from FlowFile Repository...");
        final int partitionCount = Integer.parseInt(nifiProps.getProperty("nifi.flowfile.repository.partitions", "256"));

        // We need to create the SerDe and we need to give it the 'queue map'. We don't actually know the ID's of the queues, though,
        // so we inject a special 'DummyQueueMap' that just creates a queue on demand when requested. We can do this because the only thing
        // that we actually need the queue for is so that we can obtain it via a call to getFlowFileQueue. But we don't do that, so it doesn't
        // really matter. But we can't return null or we will have some problems... we also can't override the getFlowFileQueue method
        // of the serde because if we do that, then when the write-ahead log writes out the class name of the serde, it'll be the wrong class
        // name, as the class name will be that of the overriding class.
        final SchemaRepositoryRecordSerde serde = new SchemaRepositoryRecordSerde(claimManager);
        final Method method = RepositoryRecordSerde.class.getDeclaredMethod("setQueueMap", Map.class);
        method.setAccessible(true);
        final Map<String, FlowFileQueue> queueMap = new DummyQueueMap();
        method.invoke(serde, queueMap);

        final SyncListener syncListener = null;
        final MinimalLockingWriteAheadLog<RepositoryRecord> originalWal = new MinimalLockingWriteAheadLog<>(originalDir.toPath(), partitionCount, serde, syncListener);
        final MinimalLockingWriteAheadLog<RepositoryRecord> destinationWal = new MinimalLockingWriteAheadLog<>(destinationDir.toPath(), partitionCount, serde, syncListener);
        destinationWal.recoverRecords();

        int removed = 0;
        final Collection<RepositoryRecord> recordList = originalWal.recoverRecords();
        for (final RepositoryRecord record : recordList) {
            final ContentClaim currentClaim = record.getCurrentClaim();

            // No content claim so keep the record.
            if (currentClaim == null) {
                // Keep Repository Record!
                destinationWal.update(Collections.singleton(record), false);
                continue;
            }

            // Check if the content claim exists
            final ResourceClaim resourceClaim = currentClaim.getResourceClaim();
            final int claimCount = claimManager.getClaimantCount(resourceClaim);
            if (claimCount == 0) {
                removed++;
                continue;
            }

            // Claim exists so keep the Repository Record.
            destinationWal.update(Collections.singleton(record), false);
        }

        originalWal.shutdown();
        destinationWal.shutdown();

        System.out.println("Copied FlowFile Repository over (other than swap files), removing " + removed + " FlowFiles whose content could not be found");
        System.out.println("Scanning Swap Files...");

        final File originalSwapDir = new File(originalDir, "swap");
        final File destinationSwapDir = new File(destinationDir, "swap");

        final File[] originalSwapFiles = originalSwapDir.listFiles();
        if (originalSwapFiles == null) {
            throw new IOException("Unable to obtain a listing of Swap Files from + " + originalSwapDir + "; will not copy over swap files!!");
        }

        int swapFlowFilesRemoved = 0;
        final FileSystemSwapManager swapManager = new FileSystemSwapManager(nifiProps);

        final SwapManagerInitializationContext initializationContext = new SwapManagerInitializationContext() {
            @Override
            public ResourceClaimManager getResourceClaimManager() {
                return claimManager;
            }

            @Override
            public FlowFileRepository getFlowFileRepository() {
                return null;
            }

            @Override
            public EventReporter getEventReporter() {
                return null;
            }
        };

        swapManager.initialize(initializationContext);

        for (final File originalSwapFile : originalSwapFiles) {
            final File destinationSwapFile = new File(destinationSwapDir, originalSwapFile.getName());
            swapFlowFilesRemoved += processSwapFile(originalSwapFile, destinationSwapFile, claimManager, swapManager);
        }

        System.out.println("Completed recovery of FlowFile Repository; " + swapFlowFilesRemoved + " FlowFiles were removed");
    }

    private static int processSwapFile(final File originalSwapFile, final File destinationSwapFile, final ResourceClaimManager claimManager, final FileSystemSwapManager swapManager)
        throws IOException {
        System.out.print("Processing Swap File " + originalSwapFile + "... ");
        System.out.flush();
        int removed = 0;

        final String swapLocation = originalSwapFile.getAbsolutePath();
        final SwapContents swapContents = swapManager.peek(swapLocation, null);

        final List<FlowFileRecord> toWriteOut = new ArrayList<>();
        for (final FlowFileRecord flowFile : swapContents.getFlowFiles()) {
            final ContentClaim contentClaim = flowFile.getContentClaim();
            if (contentClaim == null) {
                toWriteOut.add(flowFile);
                continue;
            }

            final int claimantCount = claimManager.getClaimantCount(contentClaim.getResourceClaim());
            if (claimantCount > 0) {
                toWriteOut.add(flowFile);
                continue;
            }

            removed++;
        }

        if (toWriteOut.isEmpty()) {
            System.out.println("Completed; dropped all FlowFiles because all were missing content");
            return swapContents.getSummary().getQueueSize().getObjectCount();
        }

        final File destinationSwapDir = destinationSwapFile.getParentFile();
        if (!destinationSwapDir.exists() && !destinationSwapDir.mkdirs()) {
            throw new IOException("Could not create destination swap directory " + destinationSwapDir);
        }

        final SwapSerializer serializer = new SchemaSwapSerializer();
        try (final FileOutputStream fos = new FileOutputStream(destinationSwapFile);
            final OutputStream out = new BufferedOutputStream(fos)) {
            out.write(SWAP_MAGIC_HEADER);
            final DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF(serializer.getSerializationName());

            final Pattern queueIdPattern = Pattern.compile("\\d+-(.*?)-.*");
            final Matcher matcher = queueIdPattern.matcher(originalSwapFile.getName());
            final boolean matches = matcher.matches();
            if (!matches) {
                throw new IOException("Swap File " + originalSwapFile + " has wrong name. It should include queue identifier but the ID could not be extracted");
            }

            final String queueId = matcher.group(1);
            final FlowFileQueue flowFileQueue = new DummyFlowFileQueue(queueId);

            serializer.serializeFlowFiles(toWriteOut, flowFileQueue, swapLocation, out);
        }

        System.out.println("Completed; dropped " + removed + " FlowFiles with missing content");
        return removed;
    }



    private static void populateClaimManager(final String containerName, final File containerDir, final ResourceClaimManager claimManager) throws IOException {
        System.out.print("Scanning Container " + containerName + " (" + containerDir + ")... ");
        System.out.flush();

        final File[] sectionDirs = containerDir.listFiles();
        if (sectionDirs == null) {
            throw new IOException("Could not obtain listing of directory " + containerDir);
        }

        int counter = 0;
        for (final File sectionDir : sectionDirs) {
            final String sectionName = sectionDir.getName();

            final String[] resourceFileNames = sectionDir.list();
            if (resourceFileNames == null) {
                throw new IOException("Could not obtain listing of directory " + sectionDir);
            }

            for (final String resourceFileName : resourceFileNames) {
                final ResourceClaim resourceClaim = claimManager.newResourceClaim(containerName, sectionName, resourceFileName, false, false);
                claimManager.incrementClaimantCount(resourceClaim);
                counter++;
            }
        }

        System.out.println("Found " + counter + " Resource Claims in this Container");
    }

    private static String substringAfter(final String input, final String search) {
        final int lastIndex = input.lastIndexOf(search);
        if (lastIndex < 0) {
            return input;
        }

        return input.substring(lastIndex + search.length());
    }

    private static boolean verifyDirectory(final File dir) {
        if (!dir.exists() || !dir.canRead() || !dir.isDirectory()) {
            System.out.println("Cannot find or cannot read " + dir + " or it is not a directory");
            return false;
        }

        return true;
    }

}
