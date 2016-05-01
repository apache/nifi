package org.apache.nifi.processors.azure;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public abstract class AbstractAzureBlobProcessor extends AbstractAzureProcessor {

    public static final PropertyDescriptor BLOB = new PropertyDescriptor.Builder().name("Blob").description("The path of the blob to download").addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true).required(true).build();

    public static final PropertyDescriptor BLOB_TYPE = new PropertyDescriptor.Builder()
            .name("Blob type")
            .description("Blobs can be block type of page type")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(AzureConstants.BLOCK, AzureConstants.PAGE)
            .defaultValue(AzureConstants.BLOCK.getValue())
            .required(true).build();

    public static final List<PropertyDescriptor> properties = Collections
            .unmodifiableList(Arrays.asList(AzureConstants.ACCOUNT_NAME, AzureConstants.ACCOUNT_KEY, AzureConstants.CONTAINER, BLOB, BLOB_TYPE));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Get a reference to a blob based on the type.
     * 
     * @param container
     * @param blobType
     * @param blobPath
     * @return
     * @throws URISyntaxException
     * @throws StorageException
     */
    protected CloudBlob getBlob(CloudBlobContainer container, String blobType, String blobPath) throws URISyntaxException, StorageException {
        if (blobType == AzureConstants.BLOCK.getValue()) {
            return container.getBlockBlobReference(blobPath);
        }
        return container.getPageBlobReference(blobPath);
    }

}
