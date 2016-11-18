package org.apache.nifi.processors.azure.storage;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.processors.azure.AzureConstants;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@SeeAlso({ ListAzureBlobStorage.class, FetchAzureBlobStorage.class })
@CapabilityDescription("Puts content into an Azure Storage Blob")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({ @WritesAttribute(attribute = "azure.container", description = "The name of the azure container"),
        @WritesAttribute(attribute = "azure.blobname", description = "The name of the azure blob"), 
        @WritesAttribute(attribute = "azure.primaryUri", description = "Primary location for blob content"),
        @WritesAttribute(attribute = "azure.etag", description = "Etag for the Azure blob"), 
        @WritesAttribute(attribute = "azure.length", description = "Length of the blob"),
        @WritesAttribute(attribute = "azure.timestamp", description = "The timestamp in Azure for the blob"),
        @WritesAttribute(attribute = "azure.blobtype", description = "This is the type of blob and can be either page or block type") })
public class PutAzureBlobStorage extends AbstractAzureBlobProcessor {

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
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

            CloudBlob blob = getBlob(container, blobType, blobPath);

            final Map<String, String> attributes = new HashMap<>();
            long length = flowFile.getSize();
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream rawIn) throws IOException {
                    final InputStream in = new BufferedInputStream(rawIn);
                    try {
                        blob.upload(in, length);
                        BlobProperties properties = blob.getProperties();
                        attributes.put("azure.container", containerName);
                        attributes.put("azure.primaryUri", blob.getQualifiedUri().toString());
                        attributes.put("azure.etag", properties.getEtag());
                        attributes.put("azure.length", String.valueOf(length));
                        attributes.put("azure.blobtype", blobType);
                        attributes.put("azure.timestamp", String.valueOf(properties.getLastModified()));
                    } catch (StorageException | URISyntaxException e) {
                        throw new IOException(e);
                    }
                }
            });

            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }
            session.transfer(flowFile, REL_SUCCESS);

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, blob.getQualifiedUri().toString(), transferMillis);

        } catch (IllegalArgumentException | URISyntaxException | StorageException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
