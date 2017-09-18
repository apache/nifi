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
package org.apache.nifi.processors.adls;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.DirectoryEntryType;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.adls.ADLSConstants.ACCOUNT_NAME;
import static org.apache.nifi.processors.adls.ADLSConstants.REL_SUCCESS;

@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"hadoop", "ADLS", "get", "list", "ingest", "ingress", "source", "filesystem"})
@CapabilityDescription("Retrieves a listing of files from ADLS. For each file that is "
        + "listed in ADLS, this processor creates a FlowFile that represents the ADLS file to be fetched in conjunction with FetchADLSFile. This Processor is "
        +  "designed to run on Primary Node only in a cluster. If the primary node changes, the new Primary Node will pick up where the previous node left "
        +  "off without duplicating all of the data.")
@WritesAttributes({
        @WritesAttribute(attribute="filename", description="The name of the file that was read from ADLS"),
        @WritesAttribute(attribute="path", description="The path is set to the path of the file on ADLS"),
        @WritesAttribute(attribute="adls.owner", description="The user that owns the file"),
        @WritesAttribute(attribute="adls.group", description="The group that owns the file"),
        @WritesAttribute(attribute="adls.lastModified", description="The timestamp of when the file was last modified, as milliseconds since midnight Jan 1, 1970 UTC"),
        @WritesAttribute(attribute="adls.length", description="The number of bytes in the file"),
        @WritesAttribute(attribute="adls.expirytime", description="The number of bytes in the file"),
        @WritesAttribute(attribute="adls.permissions", description="The permissions for the file in ADLS. This is formatted as 3 characters for the owner, "
                + "3 for the group, and 3 for other users. For example rw-rw-r--")
})
@Stateful(scopes = Scope.CLUSTER, description = "After performing a listing of ADLS files, the latest timestamp of all the files transferred is stored. "
        + "This allows the Processor to list only files that have been added or modified after "
        + "this date the next time that the Processor is run, without having to store all of the actual filenames/paths which could lead to performance "
        + "problems. State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary "
        + "Node is selected, the new node can pick up where the previous node left off, without duplicating the data.")
public class ListADLSFile extends ADLSAbstractProcessor {

    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Distributed Cache Service")
            .description("Specifies the Controller Service that should be used to maintain state about what has been pulled from ADLS so that if a new node "
                    + "begins pulling data, it won't duplicate all of the work that has been done.")
            .required(false)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor RECURSE_SUBDIRS = new PropertyDescriptor.Builder()
            .name("Recurse Subdirectories")
            .description("Indicates whether to list files from subdirectories of the ADLS directory")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue("[^\\.].*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The ADLS directory from which files should be listed")
            .required(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    // increase this factor from test case to test minimum duration between runs functionality
    protected long testingMinimumDurationMultiplicationFactor = 1;

    private volatile long latestTimestampListed = 0;
    private volatile long lastRunTimestamp = 0;

    protected static final String LISTING_TIMESTAMP_KEY = "adls.listing.timestamp";

    protected static final long MINIMUM_DURATON_BW_RUNS_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        super.descriptors.add(DISTRIBUTED_CACHE_SERVICE);
        super.descriptors.add(RECURSE_SUBDIRS);
        super.descriptors.add(FILE_FILTER);
        super.descriptors.add(DIRECTORY);
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        if (isConfigurationRestored()
                && (descriptor.equals(DIRECTORY)
                || descriptor.equals(FILE_FILTER)
                || descriptor.equals(ACCOUNT_NAME))) {
            latestTimestampListed = 0;
        }
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        final long now = System.nanoTime();
        if (now - lastRunTimestamp < MINIMUM_DURATON_BW_RUNS_NANOS * testingMinimumDurationMultiplicationFactor) {
            processContext.yield();
            return;
        }
        lastRunTimestamp = now;

        final String fileFilter = processContext.getProperty(FILE_FILTER).getValue();
        final String directoryValue = processContext.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        final String directory = Paths.get(directoryValue).normalize().toString();
        final Date latestTimestampListedDate;
        try {
            latestTimestampListed = getLatestTimestampListedFromState(processContext);
            latestTimestampListedDate = new Date(latestTimestampListed);
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve timestamp of last listing from Distributed Cache Service. Will not perform listing until this is accomplished.");
            processContext.yield();
            return;
        }

        ADLStoreClient adlStoreClient = getAdlStoreClient();
        if(adlStoreClient == null) {
            createADLSClient(processContext);
            adlStoreClient = getAdlStoreClient();
        }
        final boolean recursive = processContext.getProperty(RECURSE_SUBDIRS).asBoolean();

        List<DirectoryEntry> directoryEntries = null;
        try {
            if(directory == null || directory.isEmpty())
                throw new IOException("The ADLS directory from which files should be listed is empty");
            directoryEntries = getDirectoryListing(adlStoreClient, directory, recursive);
        } catch (IOException e) {
            getLogger().error("Failed to perform listing of ADLS due to {}", new Object[] {e});
            return;
        }

        if(directoryEntries.size() == 0) {
            getLogger().debug("There is no data to list. Yielding.");
            processContext.yield();
            return;
        }

        Optional<DirectoryEntry> optionalDirectoryEntry = directoryEntries
                .stream()
                .filter(directoryEntry ->
                        directoryEntry.type.equals(DirectoryEntryType.FILE)
                )
                .filter(directoryEntry ->
                        directoryEntry.lastModifiedTime.after(latestTimestampListedDate)
                )
                .filter(directoryEntry ->
                        directoryEntry.name.matches(fileFilter)
                )
                .map(directoryEntry -> {
                    createAndTransferFlowFile(processSession, directoryEntry);
                    return directoryEntry;
                })
                .max(Comparator.comparing(de -> de.lastModifiedTime));

        Date currentRunLatestModifiedTimestamp;
        try {
            currentRunLatestModifiedTimestamp = optionalDirectoryEntry.get().lastModifiedTime;
        } catch (NoSuchElementException e) {
            getLogger().debug("There is no data to list. Yielding.");
            processContext.yield();
            return;
        }

        if(currentRunLatestModifiedTimestamp.after(latestTimestampListedDate)) {
            processSession.commit();
            getLogger().debug("Session commited");
            try {
                saveState(processContext, currentRunLatestModifiedTimestamp);
            } catch (IOException e) {
                getLogger().warn("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", e);
            }
        }
    }

    private void saveState(ProcessContext processContext, final Date listedTimestamp) throws IOException {
        final Map<String, String> updatedState = new HashMap<>(1);
        updatedState.put(LISTING_TIMESTAMP_KEY, String.valueOf(listedTimestamp.getTime()));
        processContext.getStateManager().setState(updatedState, Scope.CLUSTER);
        getLogger().debug("New state saved with listed time: {}", new Object[] {listedTimestamp.toString()});
    }

    private void createAndTransferFlowFile(ProcessSession processSession, DirectoryEntry directoryEntry) {
        final Map<String, String> attributes = createAttributes(directoryEntry);
        FlowFile flowFile = processSession.create();
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        processSession.transfer(flowFile, REL_SUCCESS);
    }

    private Map<String, String> createAttributes(final DirectoryEntry directoryEntry) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.FILENAME.key(), directoryEntry.name);
        Path fullNamePath = Paths.get(directoryEntry.fullName);
        fullNamePath.toAbsolutePath().getParent().toString();
        attributes.put(CoreAttributes.PATH.key(), fullNamePath.getParent().toString());
        attributes.put(CoreAttributes.ABSOLUTE_PATH.key(), fullNamePath.toAbsolutePath().getParent().toString());
        attributes.put("adls.owner", directoryEntry.user);
        attributes.put("adls.group", directoryEntry.group);
        attributes.put("adls.lastModified", String.valueOf(directoryEntry.lastModifiedTime));
        attributes.put("adls.length", String.valueOf(directoryEntry.length));
        attributes.put("adls.permissions", directoryEntry.permission);
        if(directoryEntry.expiryTime != null)
            attributes.put("adls.expirytime", String.valueOf(directoryEntry.expiryTime));
        return attributes;
    }

    private List<DirectoryEntry> getDirectoryListing(ADLStoreClient client, String directoryPath, boolean recursive) throws IOException {

        if(!recursive)
            return client.enumerateDirectory(directoryPath);
        else {
            List<DirectoryEntry> directoryEntryList = new ArrayList<>();
            flattenDirectoryEntriesRecursively(client, directoryPath, directoryEntryList);
            return directoryEntryList;
        }
    }

    private void flattenDirectoryEntriesRecursively(
            ADLStoreClient client,
            String directoryPath,
            List<DirectoryEntry> directoryEntriesList) throws IOException {
        for (DirectoryEntry directoryEntry:
                client.enumerateDirectory(directoryPath)) {
            if(directoryEntry.type.equals(DirectoryEntryType.FILE))
                directoryEntriesList.add(directoryEntry);
            else if(directoryEntry.type.equals(DirectoryEntryType.DIRECTORY)) {
                flattenDirectoryEntriesRecursively(client, directoryEntry.fullName, directoryEntriesList);
            }
        };
    }

    private long getLatestTimestampListedFromState(ProcessContext processContext) throws IOException {
        final StateMap stateMap = processContext.getStateManager().getState(Scope.CLUSTER);
        long latestTimestampListed;
        if (stateMap.getVersion() == 0) {
            latestTimestampListed = 0;
            getLogger().debug("Found no state stored");
        } else {
            final String listingTimestampString = stateMap.get(LISTING_TIMESTAMP_KEY);
            if (listingTimestampString != null) {
                latestTimestampListed = Long.parseLong(listingTimestampString);;
            } else {
                latestTimestampListed = 0;
            }
        }
        return latestTimestampListed;
    }
}
