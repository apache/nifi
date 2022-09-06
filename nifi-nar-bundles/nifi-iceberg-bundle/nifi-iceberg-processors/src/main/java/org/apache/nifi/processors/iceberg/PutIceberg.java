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
package org.apache.nifi.processors.iceberg;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"iceberg", "put", "table", "store", "record", "parse", "orc", "parquet", "avro"})
@CapabilityDescription("This processor uses Iceberg API to parse and load records into Iceberg tables. The processor supports only V2 table format." +
        "The incoming data sets are parsed with Record Reader Controller Service and ingested into an Iceberg table using the configured catalog service and provided table information." +
        "It is important that the incoming records and the Iceberg table must have matching schemas and the target Iceberg table should already exist. " +
        "The file format of the written data files are defined in the 'write.format.default' table property, if it is not provided then parquet format will be used." +
        "To avoid 'small file problem' it is recommended pre-appending a MergeRecord processor.")
@WritesAttributes({
        @WritesAttribute(attribute = "iceberg.record.count", description = "The number of records in the FlowFile")
})
public class PutIceberg extends KerberosAwareBaseProcessor {

    public static final String ICEBERG_RECORD_COUNT = "iceberg.record.count";

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor CATALOG = new PropertyDescriptor.Builder()
            .name("catalog-service")
            .displayName("Catalog Service")
            .description("Specifies the Controller Service to use for handling references to table’s metadata files.")
            .identifiesControllerService(IcebergCatalogService.class)
            .required(true)
            .build();

    static final PropertyDescriptor CATALOG_NAMESPACE = new PropertyDescriptor.Builder()
            .name("catalog-namespace")
            .displayName("Catalog namespace")
            .description("The namespace of the catalog.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table-name")
            .displayName("Iceberg table name")
            .description("The name of the Iceberg table.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the data ingestion was successful.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the data ingestion failed and retrying the operation will also fail, "
                    + "such as an invalid data or schema.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            RECORD_READER,
            CATALOG,
            TABLE_NAME,
            CATALOG_NAMESPACE,
            KERBEROS_USER_SERVICE
    ));

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> validationResults = new HashSet<>();
        if (validationContext.getProperty(TABLE_NAME).isSet() && validationContext.getProperty(CATALOG_NAMESPACE).isSet() && validationContext.getProperty(CATALOG).isSet()) {
            final Table table = initializeTable(validationContext);

            if (!validateTableVersion(table)) {
                validationResults.add(new ValidationResult.Builder().explanation("The provided table has incompatible table format. V1 table format is not supported.").build());
            }
        }

        return validationResults;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void doOnTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final Table table = initializeTable(context);

        final IcebergFileCommitter fileCommitter = new IcebergFileCommitter(table);

        IcebergTaskWriterFactory taskWriterFactory = null;
        TaskWriter<Record> taskWriter = null;
        int recordCount = 0;

        try (final InputStream in = session.read(flowFile);
             final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

            //The first record is needed from the incoming set to get the schema and initialize the task writer.
            Record firstRecord = reader.nextRecord();
            if (firstRecord != null) {
                taskWriterFactory = new IcebergTaskWriterFactory(table, firstRecord.getSchema());
                taskWriterFactory.initialize(table.spec().specId(), flowFile.getId());
                taskWriter = taskWriterFactory.create();

                taskWriter.write(firstRecord);
                recordCount++;

                //Process the remaining records
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    taskWriter.write(record);
                    recordCount++;
                }

                WriteResult result = taskWriter.complete();
                fileCommitter.commit(result);
            }
        } catch (IOException | SchemaNotFoundException | MalformedRecordException e) {
            getLogger().error("Exception occurred while writing iceberg records. Removing uncommitted data files.", e);
            try {
                if (taskWriterFactory != null) {
                    taskWriter.abort();
                }
            } catch (IOException ex) {
                throw new ProcessException("Failed to abort uncommitted data files.", ex);
            }

            session.transfer(flowFile, REL_FAILURE);
        }

        flowFile = session.putAttribute(flowFile, ICEBERG_RECORD_COUNT, String.valueOf(recordCount));
        session.transfer(flowFile, REL_SUCCESS);
    }


    private Table initializeTable(PropertyContext context) {
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);
        final String catalogNamespace = context.getProperty(CATALOG_NAMESPACE).evaluateAttributeExpressions().getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();

        final Catalog catalog = catalogService.getCatalog();

        final Namespace namespace = Namespace.of(catalogNamespace);
        final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

        return catalog.loadTable(tableIdentifier);
    }

    private boolean validateTableVersion(Table table) {
        int tableVersion = ((BaseTable) table).operations().current().formatVersion();
        return tableVersion > 1;
    }

}
