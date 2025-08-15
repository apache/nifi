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

package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@TriggerSerially
@Tags({"google", "google cloud", "bq", "bigquery"})
@CapabilityDescription("Processor to help with BigQuery table management. It can be used to create a table based on a schema as well as truncating an existing table.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class UpdateBigQueryTable extends AbstractBigQueryProcessor {

    private BigQuery client = null;

    public static final PropertyDescriptor PROJECT_ID = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(AbstractBigQueryProcessor.PROJECT_ID)
        .required(true)
        .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("Record Reader")
        .description(BigQueryAttributes.RECORD_READER_DESC)
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    public static final PropertyDescriptor CREATE_TABLE = new PropertyDescriptor.Builder()
        .name("Create Table")
        .description("If true, the processor will create the table if it does not exist using the schema provided by the configured Record Reader.")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    public static final PropertyDescriptor TRUNCATE_TABLE = new PropertyDescriptor.Builder()
        .name("Truncate Table")
        .description("If true, the processor will truncate the table if it exists.")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
        GCP_CREDENTIALS_PROVIDER_SERVICE,
        PROJECT_ID,
        DATASET,
        TABLE_NAME,
        CREATE_TABLE,
        TRUNCATE_TABLE,
        RECORD_READER,
        RETRY_COUNT,
        PROXY_CONFIGURATION_SERVICE
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
        client = getCloudService(context);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session)  {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String dataset = context.getProperty(DATASET).evaluateAttributeExpressions(flowFile).getValue();
        final String dataTableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        final boolean createTable = context.getProperty(CREATE_TABLE).asBoolean();
        final boolean truncateTable = context.getProperty(TRUNCATE_TABLE).asBoolean();

        final TableId tableId = TableId.of(dataset, dataTableName);

        try (InputStream in = session.read(flowFile);
                RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
            RecordSchema schema = reader.getSchema();

            if (createTable) {
                createTable(tableId, schema);
            }

            if (truncateTable) {
                truncateTable(tableId);
                // we penalize the flowfile as it takes time for the operation to be done
                // this is to avoid any potential error if the flowfile is then routed to
                // PutBigQuery
                session.penalize(flowFile);
            }
        } catch (Exception e) {
            getLogger().error("Error while updating BigQuery table", e);
            context.yield();
            session.transfer(flowFile, REL_FAILURE);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    private void truncateTable(TableId tableId) {
        // Get the existing table
        Table originalTable = client.getTable(tableId);
        if (originalTable != null) { // does the table exist?

            TableDefinition originalDef = originalTable.getDefinition();

            TableInfo.Builder tableBuilder = TableInfo.newBuilder(tableId, originalDef)
                    .setDescription(originalTable.getDescription())
                    .setLabels(originalTable.getLabels())
                    .setExpirationTime(originalTable.getExpirationTime());

            if (originalTable.getEncryptionConfiguration() != null) {
                tableBuilder.setEncryptionConfiguration(originalTable.getEncryptionConfiguration());
            }

            TableInfo newTableInfo = tableBuilder.build();

            // Overwrite the existing table
            if (client.delete(tableId)) {
                client.create(newTableInfo);
            }
        }
    }

    private void createTable(TableId tableId, RecordSchema schema) {
        if (client.getTable(tableId) == null) {
            // Define table
            StandardTableDefinition tableDefinition = StandardTableDefinition.of(convertRecordSchema(schema));
            TableInfo tableInfo = TableInfo.of(tableId, tableDefinition);

            // Create the table
            client.create(tableInfo);
        }
    }

    private Schema convertRecordSchema(RecordSchema recordSchema) {
        List<Field> bigQueryFields = new ArrayList<>();

        for (RecordField recordField : recordSchema.getFields()) {
            bigQueryFields.add(getBQField(recordField));
        }

        return Schema.of(bigQueryFields);
    }

    private Field getBQField(RecordField recordField) {
        final String fieldName = recordField.getFieldName();
        final DataType dataType = recordField.getDataType();
        final Field.Mode mode = recordField.isNullable() ? Field.Mode.NULLABLE : Field.Mode.REQUIRED;

        switch (dataType.getFieldType()) {
        case STRING, CHAR, ENUM, UUID:
            return Field.newBuilder(fieldName, StandardSQLTypeName.STRING).setMode(mode).build();
        case INT, LONG, BIGINT, SHORT, BYTE:
            return Field.newBuilder(fieldName, StandardSQLTypeName.INT64).setMode(mode).build();
        case FLOAT, DOUBLE:
            return Field.newBuilder(fieldName, StandardSQLTypeName.FLOAT64).setMode(mode).build();
        case BOOLEAN:
            return Field.newBuilder(fieldName, StandardSQLTypeName.BOOL).setMode(mode).build();
        case DATE:
            return Field.newBuilder(fieldName, StandardSQLTypeName.DATE).setMode(mode).build();
        case TIME:
            return Field.newBuilder(fieldName, StandardSQLTypeName.TIME).setMode(mode).build();
        case TIMESTAMP:
            return Field.newBuilder(fieldName, StandardSQLTypeName.TIMESTAMP).setMode(mode).build();
        case DECIMAL:
            return Field.newBuilder(fieldName, StandardSQLTypeName.NUMERIC).setMode(mode).build();
        case RECORD:
            if (dataType instanceof RecordDataType rdt) {
                RecordSchema subSchema = rdt.getChildSchema();
                Field[] recordFields = new Field[subSchema.getFields().size()];
                int i = 0;
                for (RecordField subField : subSchema.getFields()) {
                    recordFields[i] = getBQField(subField);
                    i++;
                }
                return Field.newBuilder(fieldName, LegacySQLTypeName.RECORD, recordFields).setMode(mode).build();
            } else {
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
            }
        case ARRAY:
            if (dataType instanceof ArrayDataType adt) {
                DataType elementType = adt.getElementType();
                Field elementField = getBQField(new RecordField(fieldName, elementType, recordField.isNullable()));
                return Field.newBuilder(fieldName, elementField.getType())
                        .setMode(Field.Mode.REPEATED)
                        .build();
            } else {
                throw new IllegalArgumentException("Invalid ARRAY type: " + dataType);
            }
        case CHOICE:
            return Field.newBuilder(fieldName, StandardSQLTypeName.STRING).setMode(mode).build();
        case MAP:
            return Field.newBuilder(fieldName, StandardSQLTypeName.STRING).setMode(mode).build();
        }

        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }

}
