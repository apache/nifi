package org.apache.nifi.processors.azure.storage;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AzureConstants;
import org.apache.nifi.processors.azure.storage.utils.BlobInfo;
import org.apache.nifi.processors.azure.storage.utils.BlobInfo.Builder;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

@TriggerSerially
@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@SeeAlso({ FetchAzureBlobStorage.class })
@CapabilityDescription("Lists blobs in an Azure Storage container. Listing details are attached to an empty FlowFile for use with FetchAzureBlobStorage")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({ @WritesAttribute(attribute = "azure.container", description = "The name of the azure container"),
        @WritesAttribute(attribute = "azure.blobname", description = "The name of the azure blob"), @WritesAttribute(attribute = "azure.primaryUri", description = "Primary location for blob content"),
        @WritesAttribute(attribute = "azure.secondaryUri", description = "Secondary location for blob content"), @WritesAttribute(attribute = "azure.etag", description = "Etag for the Azure blob"),
        @WritesAttribute(attribute = "azure.length", description = "Length of the blob"), @WritesAttribute(attribute = "azure.timestamp", description = "The timestamp in Azure for the blob"),
        @WritesAttribute(attribute = "mime.type", description = "MimeType of the content"), @WritesAttribute(attribute = "lang", description = "Language code for the content"),
        @WritesAttribute(attribute = "azure.blobtype", description = "This is the type of blob and can be either page or block type") })
@Stateful(scopes = { Scope.LOCAL, Scope.CLUSTER }, description = "After performing a listing of blobs, the timestamp of the newest blob is stored. "
        + "This allows the Processor to list only blobs that have been added or modified after " + "this date the next time that the Processor is run.")
public class ListAzureBlobStorage extends AbstractProcessor {

    private volatile Long lastListingTime = null;
    private volatile Long lastProcessedTime = 0L;
    private volatile Long lastRunTime = 0L;
    private volatile boolean justElectedPrimaryNode = false;
    private volatile boolean resetState = false;

    /*
     * A constant used in determining an internal "yield" of processing files. Given the logic to provide a pause on the newest
     * files according to timestamp, it is ensured that at least the specified millis has been eclipsed to avoid getting scheduled
     * near instantaneously after the prior iteration effectively voiding the built in buffer
     */
    static final long LISTING_LAG_NANOS = TimeUnit.MILLISECONDS.toNanos(100L);
    static final String LISTING_TIMESTAMP_KEY = "listing.timestamp";
    static final String PROCESSED_TIMESTAMP_KEY = "processed.timestamp";

    
    private static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder().name("Prefix").description("Search prefix for listing").addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true).required(false).build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received are routed to success")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(AzureConstants.ACCOUNT_NAME, AzureConstants.ACCOUNT_KEY, AzureConstants.CONTAINER, PREFIX));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (isConfigurationRestored() && isListingResetNecessary(descriptor)) {
            resetTimeStates(); // clear lastListingTime so that we have to fetch new time
            resetState = true;
        }
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeChange(final PrimaryNodeState newState) {
        justElectedPrimaryNode = (newState == PrimaryNodeState.ELECTED_PRIMARY_NODE);
    }
    
    @OnScheduled
    public final void updateState(final ProcessContext context) throws IOException {
        // Check if state already exists for this path. If so, we have already migrated the state.
        final StateMap stateMap = context.getStateManager().getState(getStateScope(context));
        
        // When scheduled to run, check if the associated timestamp is null, signifying a clearing of state and reset the internal timestamp
        if (lastListingTime != null && stateMap.get(LISTING_TIMESTAMP_KEY) == null) {
            getLogger().info("Detected that state was cleared for this component.  Resetting internal values.");
            resetTimeStates();
        }
        if (resetState) {
            context.getStateManager().clear(getStateScope(context));
            resetState = false;
        }
    }

    private void persist(final long listingTimestamp, final long processedTimestamp, final StateManager stateManager, final Scope scope) throws IOException {
        final Map<String, String> updatedState = new HashMap<>(1);
        updatedState.put(LISTING_TIMESTAMP_KEY, String.valueOf(listingTimestamp));
        updatedState.put(PROCESSED_TIMESTAMP_KEY, String.valueOf(processedTimestamp));
        stateManager.setState(updatedState, scope);
    }

    protected String getKey(final String directory) {
        return getIdentifier() + ".lastListingTime." + directory;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        Long minTimestamp = lastListingTime;

        if (this.lastListingTime == null || this.lastProcessedTime == null || justElectedPrimaryNode) {
            try {
                // Attempt to retrieve state from the state manager if a last listing was not yet established or
                // if just elected the primary node
                final StateMap stateMap = context.getStateManager().getState(getStateScope(context));
                final String listingTimestampString = stateMap.get(LISTING_TIMESTAMP_KEY);
                final String lastProcessedString= stateMap.get(PROCESSED_TIMESTAMP_KEY);
                if (lastProcessedString != null) {
                    this.lastProcessedTime = Long.parseLong(lastProcessedString);
                }
                if (listingTimestampString != null) {
                    minTimestamp = Long.parseLong(listingTimestampString);
                    // If our determined timestamp is the same as that of our last listing, skip this execution as there are no updates
                    if (minTimestamp == this.lastListingTime) {
                        context.yield();
                        return;
                    } else {
                        this.lastListingTime = minTimestamp;
                    }
                }
                justElectedPrimaryNode = false;
            } catch (final IOException ioe) {
                getLogger().error("Failed to retrieve timestamp of last listing from the State Manager. Will not perform listing until this is accomplished.");
                context.yield();
                return;
            }
        }

        final List<BlobInfo> entityList;
        try {
            // track of when this last executed for consideration of the lag nanos
            entityList = performListing(context, minTimestamp);
        } catch (final IOException e) {
            getLogger().error("Failed to perform listing on remote host due to {}", e);
            context.yield();
            return;
        }

        if (entityList == null || entityList.isEmpty()) {
            context.yield();
            return;
        }

        Long latestListingTimestamp = null;
        final TreeMap<Long, List<BlobInfo>> orderedEntries = new TreeMap<>();

        // Build a sorted map to determine the latest possible entries
        for (final BlobInfo entity : entityList) {
            final long entityTimestamp = entity.getTimestamp();
            // New entries are all those that occur at or after the associated timestamp
            final boolean newEntry = minTimestamp == null || entityTimestamp >= minTimestamp && entityTimestamp > lastProcessedTime;

            if (newEntry) {
                List<BlobInfo> entitiesForTimestamp = orderedEntries.get(entity.getTimestamp());
                if (entitiesForTimestamp == null) {
                    entitiesForTimestamp = new ArrayList<BlobInfo>();
                    orderedEntries.put(entity.getTimestamp(), entitiesForTimestamp);
                }
                entitiesForTimestamp.add(entity);
            }
        }

        int flowfilesCreated = 0;

        if (orderedEntries.size() > 0) {
            latestListingTimestamp = orderedEntries.lastKey();

            // If the last listing time is equal to the newest entries previously seen,
            // another iteration has occurred without new files and special handling is needed to avoid starvation
            if (latestListingTimestamp.equals(lastListingTime)) {
                /* We are done when either:
                 *   - the latest listing timestamp is If we have not eclipsed the minimal listing lag needed due to being triggered too soon after the last run
                 *   - the latest listing timestamp is equal to the last processed time, meaning we handled those items originally passed over
                 */
                if (System.nanoTime() - lastRunTime < LISTING_LAG_NANOS || latestListingTimestamp.equals(lastProcessedTime)) {
                    context.yield();
                    return;
                }
            } else {
                // Otherwise, newest entries are held back one cycle to avoid issues in writes occurring exactly when the listing is being performed to avoid missing data
                orderedEntries.remove(latestListingTimestamp);
            }

            for (List<BlobInfo> timestampEntities : orderedEntries.values()) {
                for (BlobInfo entity : timestampEntities) {
                    // Create the FlowFile for this path.
                    final Map<String, String> attributes = createAttributes(entity, context);
                    FlowFile flowFile = session.create();
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    session.transfer(flowFile, REL_SUCCESS);
                    flowfilesCreated++;
                }
            }
        }

        // As long as we have a listing timestamp, there is meaningful state to capture regardless of any outputs generated
        if (latestListingTimestamp != null) {
            boolean processedNewFiles = flowfilesCreated > 0;
            if (processedNewFiles) {
                // If there have been files created, update the last timestamp we processed
                lastProcessedTime = orderedEntries.lastKey();
                getLogger().info("Successfully created listing with {} new objects", new Object[]{flowfilesCreated});
                session.commit();
            }

            lastRunTime = System.nanoTime();

            if (!latestListingTimestamp.equals(lastListingTime) || processedNewFiles) {
                // We have performed a listing and pushed any FlowFiles out that may have been generated
                // Now, we need to persist state about the Last Modified timestamp of the newest file
                // that we evaluated. We do this in order to avoid pulling in the same file twice.
                // However, we want to save the state both locally and remotely.
                // We store the state remotely so that if a new Primary Node is chosen, it can pick up where the
                // previously Primary Node left off.
                // We also store the state locally so that if the node is restarted, and the node cannot contact
                // the distributed state cache, the node can continue to run (if it is primary node).
                try {
                    lastListingTime = latestListingTimestamp;
                    persist(latestListingTimestamp, lastProcessedTime, context.getStateManager(), getStateScope(context));
                } catch (final IOException ioe) {
                    getLogger().warn("Unable to save state due to {}. If NiFi is restarted before state is saved, or "
                        + "if another node begins executing this Processor, data duplication may occur.", ioe);
                }
            }

        } else {
            getLogger().debug("There is no data to list. Yielding.");
            context.yield();

            // lastListingTime = 0 so that we don't continually poll the distributed cache / local file system
            if (lastListingTime == null) {
                lastListingTime = 0L;
            }

            return;
        }
    }

    private void resetTimeStates() {
        lastListingTime = null;
        lastProcessedTime = 0L;
        lastRunTime = 0L;
    }
    
    private boolean isListingResetNecessary(PropertyDescriptor descriptor) {
        return true;
    }

    protected Map<String, String> createAttributes(BlobInfo entity, ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("azure.etag", entity.getEtag());
        attributes.put("azure.primaryUri", entity.getPrimaryUri());
        attributes.put("azure.secondaryUri", entity.getSecondaryUri());
        attributes.put("azure.blobname", entity.getName());
        attributes.put("azure.blobtype", entity.getBlobType());
        attributes.put("azure.length", String.valueOf(entity.getLength()));
        attributes.put("azure.timestamp", String.valueOf(entity.getTimestamp()));
        attributes.put("mime.type", entity.getContentType());
        attributes.put("lang", entity.getContentLanguage());

        return attributes;
    }

    protected Scope getStateScope(final ProcessContext context) {
        return Scope.CLUSTER;
    }

    protected List<BlobInfo> performListing(final ProcessContext context, final Long minTimestamp) throws IOException {
        String containerName = context.getProperty(AzureConstants.CONTAINER).evaluateAttributeExpressions().getValue();
        String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions().getValue();
        if (prefix == null) {
            prefix = "";
        }
        final List<BlobInfo> listing = new ArrayList<>();
        try {
            CloudStorageAccount storageAccount = createStorageConnection(context);
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer container = blobClient.getContainerReference(containerName);

            BlobRequestOptions blobRequestOptions = null;
            OperationContext operationContext = null;

            for (ListBlobItem blob : container.listBlobs(prefix, true, EnumSet.of(BlobListingDetails.METADATA), blobRequestOptions, operationContext)) {
                if (blob instanceof CloudBlob) {
                    CloudBlob cloudBlob = (CloudBlob) blob;
                    BlobProperties properties = cloudBlob.getProperties();
                    StorageUri uri = cloudBlob.getQualifiedStorageUri();

                    Builder builder = new BlobInfo.Builder().name(cloudBlob.getName()).primaryUri(uri.getPrimaryUri().toString()).secondaryUri(uri.getSecondaryUri().toString()).contentType(properties.getContentType())
                            .contentLanguage(properties.getContentLanguage()).etag(properties.getEtag()).lastModifiedTime(properties.getLastModified().getTime()).length(properties.getLength());

                    if (blob instanceof CloudBlockBlob) {
                        builder.blobType(AzureConstants.BLOCK.getValue());
                    } else {
                        builder.blobType(AzureConstants.PAGE.getValue());
                    }
                    listing.add(builder.build());
                }
            }
        } catch (IllegalArgumentException | URISyntaxException | StorageException e) {
            throw (new IOException(e));
        }
        return listing;
    }

    protected static CloudStorageAccount createStorageConnection(ProcessContext context) {
        final String accountName = context.getProperty(AzureConstants.ACCOUNT_NAME).evaluateAttributeExpressions().getValue();
        final String accountKey = context.getProperty(AzureConstants.ACCOUNT_KEY).evaluateAttributeExpressions().getValue();
        final String storageConnectionString = String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s", accountName, accountKey);
        try {
            return createStorageAccountFromConnectionString(storageConnectionString);
        } catch (InvalidKeyException | URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Validates the connection string and returns the storage account. The connection string must be in the Azure connection string format.
     *
     * @param storageConnectionString
     *            Connection string for the storage service or the emulator
     * @return The newly created CloudStorageAccount object
     *
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     */
    private static CloudStorageAccount createStorageAccountFromConnectionString(String storageConnectionString) throws IllegalArgumentException, URISyntaxException, InvalidKeyException {

        CloudStorageAccount storageAccount;
        try {
            storageAccount = CloudStorageAccount.parse(storageConnectionString);
        } catch (IllegalArgumentException | URISyntaxException e) {
            System.out.println("\nConnection string specifies an invalid URI.");
            System.out.println("Please confirm the connection string is in the Azure connection string format.");
            throw e;
        } catch (InvalidKeyException e) {
            System.out.println("\nConnection string specifies an invalid key.");
            System.out.println("Please confirm the AccountName and AccountKey in the connection string are valid.");
            throw e;
        }
        return storageAccount;
    }

}
