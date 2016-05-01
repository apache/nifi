package org.apache.nifi.processors.azure.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.processors.azure.AzureConstants;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@CapabilityDescription("Retrieves contents of an Azure Storage Blob, writing the contents to the content of the FlowFile")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "azure.length", description = "The length of the blob fetched")
})
public class FetchAzureBlobStorage extends AbstractAzureBlobProcessor {
    public static final List<PropertyDescriptor> properties = Collections
            .unmodifiableList(Arrays.asList(AzureConstants.ACCOUNT_NAME, AzureConstants.ACCOUNT_KEY, AzureConstants.CONTAINER, BLOB, BLOB_TYPE));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        String containerName = context.getProperty(AzureConstants.CONTAINER).evaluateAttributeExpressions(flowFile).getValue();
        String blobPath = context.getProperty(BLOB).evaluateAttributeExpressions(flowFile).getValue();
        String blobType = context.getProperty(BLOB_TYPE).evaluateAttributeExpressions(flowFile).getValue();

        try {
            CloudStorageAccount storageAccount = createStorageConnection(context, flowFile);
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer container = blobClient.getContainerReference(containerName);

            final Map<String, String> attributes = new HashMap<>();
            final CloudBlob blob = getBlob(container, blobType, blobPath);

            // TODO - we may be able do fancier things with ranges and
            // distribution of download over threads, investigate
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream os) throws IOException {
                    try {
                        blob.download(os);
                    } catch (StorageException e) {
                        throw new IOException(e);
                    }
                }
            });
            
            long length = blob.getProperties().getLength();
            attributes.put("azure.length", String.valueOf(length));
            
            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }

            session.transfer(flowFile, REL_SUCCESS);
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(flowFile, blob.getQualifiedUri().toString(), transferMillis);

        } catch (IllegalArgumentException | URISyntaxException | StorageException e1) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
