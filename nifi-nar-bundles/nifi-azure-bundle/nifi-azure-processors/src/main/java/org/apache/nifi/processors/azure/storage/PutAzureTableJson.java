package org.apache.nifi.processors.azure.storage;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Date;
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
import org.apache.nifi.processors.azure.AbstractAzureTableProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureTableEntity;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableResult;

/**
 * Puts JSON content from the flow file into an azure table row
 * 
 * @author Simon Elliston Ball <sball@hortonworks.com>
 *
 */
@Tags({ "azure", "microsoft", "cloud", "storage", "table" })
@SeeAlso({ GetAzureTable.class })
@CapabilityDescription("Puts an entry into an Azure Storage Table")
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({ @WritesAttribute(attribute = "azure.etag", description = "Etag for the entry") })
public class PutAzureTableJson extends AbstractAzureTableProcessor {
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final long startNanos = System.nanoTime();

        final CloudStorageAccount storageAccount = createStorageConnection(context, flowFile);
        final CloudTableClient tableClient = storageAccount.createCloudTableClient();
        final String tableName = context.getProperty(TABLE).evaluateAttributeExpressions(flowFile).getValue();
        final String partitionKey = context.getProperty(PARTITION).evaluateAttributeExpressions(flowFile).getValue();
        final String rowKey = context.getProperty(ROW).evaluateAttributeExpressions(flowFile).getValue();

        try {
            CloudTable cloudTable = tableClient.getTableReference(tableName);
            cloudTable.createIfNotExists();
            final Map<String, String> attributes = new HashMap<>();
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream rawIn) throws IOException {
                    final InputStream in = new BufferedInputStream(rawIn);
                    AzureTableEntity entity = AzureTableEntity.readFromJson(in);
                    entity.setPartitionKey(partitionKey);
                    entity.setRowKey(rowKey);
                    entity.setTimestamp(new Date());

                    TableOperation insertOrReplace = TableOperation.insertOrReplace(entity);
                    try {
                        TableResult result = cloudTable.execute(insertOrReplace);
                        attributes.put("azure.etag", result.getEtag());
                    } catch (StorageException e) {
                        throw new IOException(e);
                    }
                }
            });
            if (!attributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }
            session.transfer(flowFile, REL_SUCCESS);
            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, tableName, transferMillis);
        } catch (URISyntaxException | StorageException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
