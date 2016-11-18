package org.apache.nifi.processors.azure.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processors.azure.AbstractAzureTableProcessor;
import org.apache.nifi.processors.azure.storage.utils.AzureTableEntity;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.EdmType;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.Operators;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;

@Tags({ "azure", "microsoft", "cloud", "storage", "table" })
@CapabilityDescription("Retrieves entries from Azure Storage Tables, writing the contents of the Azure cell in Avro format to the content of the FlowFile")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
    @WritesAttribute(attribute = "azure.table", description = "The Azure table name"),
    @WritesAttribute(attribute = "azure.partitionKey", description = "Azure Table partition key"),
    @WritesAttribute(attribute = "azure.rowKey", description = "Azure Table row key"),
    @WritesAttribute(attribute = "azure.etag", description = "Etag for the entry"),
    @WritesAttribute(attribute = "azure.timestamp", description = "Azure Table timestamp")  
})
public class GetAzureTable extends AbstractAzureTableProcessor {

    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final long startNanos = System.nanoTime();

        final CloudStorageAccount storageAccount = createStorageConnection(context);
        final CloudTableClient tableClient = storageAccount.createCloudTableClient();

        final String tableName = context.getProperty(TABLE).evaluateAttributeExpressions().getValue();
        final String partitionKey = context.getProperty(PARTITION).evaluateAttributeExpressions().getValue();
        final String rowKey = context.getProperty(ROW).evaluateAttributeExpressions().getValue();

        FlowFile flowFile;
        try {
            CloudTable cloudTable = tableClient.getTableReference(tableName);
            final String PARTITION_KEY = "PartitionKey";
            final String ROW_KEY = "RowKey";

            String partitionFilter = TableQuery.generateFilterCondition(PARTITION_KEY, QueryComparisons.EQUAL, partitionKey);
            String rowFilter = TableQuery.generateFilterCondition(ROW_KEY, QueryComparisons.EQUAL, rowKey);
            String combinedFilter = TableQuery.combineFilters(partitionFilter, Operators.AND, rowFilter);

            TableQuery<AzureTableEntity> query = TableQuery.from(AzureTableEntity.class).where(combinedFilter);

            Iterable<AzureTableEntity> results = cloudTable.execute(query);
            for (AzureTableEntity result : results) {
                final Map<String, String> attributes = new HashMap<>();

                flowFile = session.create();
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream outStream) throws IOException {
                        final Schema schema = createSchema(result);
                        final GenericRecord rec = new GenericData.Record(schema);
                        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
                        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                            dataFileWriter.create(schema, outStream);
                            rec.put("PartionKey", result.getPartitionKey());
                            rec.put("RowKey", result.getRowKey());
                            rec.put("Timestamp", result.getTimestamp().getTime());

                            for (Entry<String, EntityProperty> i : result.getMap().entrySet()) {
                                EntityProperty value = i.getValue();
                                EdmType edmType = value.getEdmType();
                                if (edmType == EdmType.STRING) {
                                    rec.put(i.getKey(), value.getValueAsString());
                                } else if (edmType == EdmType.BOOLEAN) {
                                    rec.put(i.getKey(), value.getValueAsBoolean());
                                } else if (edmType == EdmType.DOUBLE || edmType == EdmType.DECIMAL) {
                                    rec.put(i.getKey(), value.getValueAsDouble());
                                } else if (edmType == EdmType.INT16 || edmType == EdmType.INT32) {
                                    rec.put(i.getKey(), value.getValueAsInteger());
                                } else if (edmType == EdmType.INT64) {
                                    rec.put(i.getKey(), value.getValueAsLong());
                                } else if (edmType == EdmType.NULL) {
                                    rec.put(i.getKey(), null);
                                } else {
                                    rec.put(i.getKey(), value.getValueAsString());
                                }
                            }
                            dataFileWriter.append(rec);
                        }
                    }

                    private Schema createSchema(AzureTableEntity result) {
                        final FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();
                        builder.name("PartitionKey").type().stringType().noDefault();
                        builder.name("RowKey").type().stringType().noDefault();
                        builder.name("Timestamp").type().longType().noDefault();

                        for (Entry<String, EntityProperty> i : result.getMap().entrySet()) {
                            EntityProperty value = i.getValue();
                            EdmType edmType = value.getEdmType();
                            if (edmType == EdmType.STRING) {
                                builder.name(i.getKey()).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                            } else if (edmType == EdmType.BOOLEAN) {
                                builder.name(i.getKey()).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion().noDefault();
                            } else if (edmType == EdmType.DOUBLE || edmType == EdmType.DECIMAL) {
                                builder.name(i.getKey()).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion().noDefault();
                            } else if (edmType == EdmType.INT16 || edmType == EdmType.INT32) {
                                builder.name(i.getKey()).type().unionOf().nullBuilder().endNull().and().intType().endUnion().noDefault();
                            } else if (edmType == EdmType.INT64) {
                                builder.name(i.getKey()).type().unionOf().nullBuilder().endNull().and().longType().endUnion().noDefault();
                            } else if (edmType == EdmType.NULL) {
                                builder.name(i.getKey()).type().unionOf().nullBuilder().endNull();
                            } else {
                                builder.name(i.getKey()).type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
                            }
                        }
                        return builder.endRecord();
                    }
                });

                attributes.put("azure.table", tableName);
                attributes.put("azure.partitionKey", result.getPartitionKey());
                attributes.put("azure.rowKey", result.getRowKey());
                attributes.put("azure.etag", result.getEtag());
                attributes.put("azure.timestamp", String.valueOf(result.getTimestamp().getTime()));
                flowFile = session.putAllAttributes(flowFile, attributes);

                session.transfer(flowFile, REL_SUCCESS);
                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().fetch(flowFile, cloudTable.getName(), transferMillis);
            }

        } catch (URISyntaxException | StorageException e) {
            context.yield();
        }
    }
}
