package org.apache.nifi.services.azure.storage;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Optional;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"azure", "microsoft", "cloud", "storage", "blob", "lookup", "enrich", "key", "value"})
@CapabilityDescription("Allows users to add key/value pairs as User-defined Properties." +
    "Each property that is added will be looked up in the User Metadata on the Blob")
public class AzureBlobStorageUserMetadataStringLookupService extends AbstractControllerService implements StringLookupService {

    protected static final String KEY = "key";
    protected static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(Stream.of(KEY).collect(Collectors.toSet()));

    private static final List<PropertyDescriptor> PROPERTIES = Collections
        .unmodifiableList(Arrays.asList(
            AzureStorageUtils.CONTAINER,
            AzureStorageUtils.STORAGE_CREDENTIALS_SERVICE,
            AzureStorageUtils.ACCOUNT_NAME,
            AzureStorageUtils.ACCOUNT_KEY,
            AzureStorageUtils.PROP_SAS_TOKEN,
            AbstractAzureBlobProcessor.BLOB
        ));

    private volatile String containerName;
    private volatile AzureStorageCredentialsService storageCredentialsService;
    private volatile ConfigurationContext context;

    private AzureStorageCredentialsDetails getStorageCredentialsDetails(Map<String, String> attributes) {
        if (storageCredentialsService != null) {
            return storageCredentialsService.getStorageCredentialsDetails(attributes);
        } else {
            return AzureStorageUtils.createStorageCredentialsDetails(context, attributes);
        }
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions().getValue();
        storageCredentialsService = context.getProperty(AzureStorageUtils.STORAGE_CREDENTIALS_SERVICE)
            .asControllerService(AzureStorageCredentialsService.class);
        this.context = context;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = AzureStorageUtils.validateCredentialProperties(validationContext);
        AzureStorageUtils.validateProxySpec(validationContext, results);
        return results;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Optional<String> lookup(Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        String key = (String) coordinates.get(KEY);
        String blobPath = getProperty(AbstractAzureBlobProcessor.BLOB).evaluateAttributeExpressions(context).getValue();

        if (blobPath.isEmpty()) {
            return Optional.empty();
        }

        try {
            AzureStorageCredentialsDetails storageCredentialsDetails = getStorageCredentialsDetails(context);
            final CloudStorageAccount cloudStorageAccount = new CloudStorageAccount(storageCredentialsDetails.getStorageCredentials(),
                true, null, storageCredentialsDetails.getStorageAccountName());
            final CloudBlobClient blobClient = cloudStorageAccount.createCloudBlobClient();
            final CloudBlobContainer container = blobClient.getContainerReference(containerName);

            final CloudBlob blob = container.getBlockBlobReference(blobPath);
            if (!blob.exists()) {
                return Optional.empty();
            }

            blob.downloadAttributes();
            String metadataValue = blob.getMetadata().get(key);

            return Optional.ofNullable(metadataValue);
        } catch (URISyntaxException | StorageException e) {
            throw new LookupFailureException(e);
        }
    }

    @Override
    public Optional<String> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        return lookup(coordinates, Collections.emptyMap());
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }
}
