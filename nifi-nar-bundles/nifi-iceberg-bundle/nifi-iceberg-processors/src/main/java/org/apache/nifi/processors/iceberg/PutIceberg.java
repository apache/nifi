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

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.Tasks;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.iceberg.converter.IcebergRecordConverter;
import org.apache.nifi.processors.iceberg.writer.IcebergTaskWriterFactory;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

@Tags({"iceberg", "put", "table", "store", "record", "parse", "orc", "parquet", "avro"})
@CapabilityDescription("This processor uses Iceberg API to parse and load records into Iceberg tables. " +
        "The incoming data sets are parsed with Record Reader Controller Service and ingested into an Iceberg table using the configured catalog service and provided table information. " +
        "The target Iceberg table should already exist and it must have matching schemas with the incoming records, " +
        "which means the Record Reader schema must contain all the Iceberg schema fields, every additional field which is not present in the Iceberg schema will be ignored. " +
        "To avoid 'small file problem' it is recommended pre-appending a MergeRecord processor.")
@WritesAttributes({
        @WritesAttribute(attribute = "iceberg.record.count", description = "The number of records in the FlowFile.")
})
public class PutIceberg extends AbstractIcebergProcessor {

    public static final String ICEBERG_RECORD_COUNT = "iceberg.record.count";

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor CATALOG_NAMESPACE = new PropertyDescriptor.Builder()
            .name("catalog-namespace")
            .displayName("Catalog Namespace")
            .description("The namespace of the catalog.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table-name")
            .displayName("Table Name")
            .description("The name of the table.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor FILE_FORMAT = new PropertyDescriptor.Builder()
            .name("file-format")
            .displayName("File Format")
            .description("File format to use when writing Iceberg data files." +
                    " If not set, then the 'write.format.default' table property will be used, default value is parquet.")
            .allowableValues(
                    new AllowableValue("AVRO"),
                    new AllowableValue("PARQUET"),
                    new AllowableValue("ORC"))
            .build();

    static final PropertyDescriptor MAXIMUM_FILE_SIZE = new PropertyDescriptor.Builder()
            .name("maximum-file-size")
            .displayName("Maximum File Size")
            .description("The maximum size that a file can be, if the file size is exceeded a new file will be generated with the remaining data." +
                    " If not set, then the 'write.target-file-size-bytes' table property will be used, default value is 512 MB.")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the data ingestion was successful.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the data ingestion failed and retrying the operation will also fail, such as an invalid data or schema.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            RECORD_READER,
            CATALOG,
            CATALOG_NAMESPACE,
            TABLE_NAME,
            FILE_FORMAT,
            MAXIMUM_FILE_SIZE,
            KERBEROS_USER_SERVICE
    ));

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void doOnTrigger(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String fileFormat = context.getProperty(FILE_FORMAT).evaluateAttributeExpressions().getValue();
        final String maximumFileSize = context.getProperty(MAXIMUM_FILE_SIZE).evaluateAttributeExpressions().getValue();

        Table table;

        try {
            table = loadTable(context);
        } catch (Exception e) {
            getLogger().error("Failed to load table from catalog", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        TaskWriter<org.apache.iceberg.data.Record> taskWriter = null;
        int recordCount = 0;

        try (final InputStream in = session.read(flowFile); final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
            final FileFormat format = getFileFormat(table.properties(), fileFormat);
            final IcebergTaskWriterFactory taskWriterFactory = new IcebergTaskWriterFactory(table, flowFile.getId(), format, maximumFileSize);
            taskWriter = taskWriterFactory.create();

            final IcebergRecordConverter recordConverter = new IcebergRecordConverter(table.schema(), reader.getSchema(), format);

            Record record;
            while ((record = reader.nextRecord()) != null) {
                taskWriter.write(recordConverter.convert(record));
                recordCount++;
            }

            final WriteResult result = taskWriter.complete();
            appendDataFiles(table, result);
        } catch (Exception e) {
            getLogger().error("Exception occurred while writing iceberg records. Removing uncommitted data files", e);
            try {
                if (taskWriter != null) {
                    abort(taskWriter.dataFiles(), table);
                }
            } catch (Exception ex) {
                getLogger().error("Failed to abort uncommitted data files", ex);
            }

            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAttribute(flowFile, ICEBERG_RECORD_COUNT, String.valueOf(recordCount));
        session.transfer(flowFile, REL_SUCCESS);
    }

    /**
     * Loads a table from the catalog service with the provided values from the property context.
     *
     * @param context holds the user provided information for the {@link Catalog} and the {@link Table}
     * @return loaded table
     */
    private Table loadTable(PropertyContext context) {
        final IcebergCatalogService catalogService = context.getProperty(CATALOG).asControllerService(IcebergCatalogService.class);
        final String catalogNamespace = context.getProperty(CATALOG_NAMESPACE).evaluateAttributeExpressions().getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();

        final Catalog catalog = catalogService.getCatalog();

        final Namespace namespace = Namespace.of(catalogNamespace);
        final TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

        return catalog.loadTable(tableIdentifier);
    }

    /**
     * Appends the pending data files to the given {@link Table}.
     *
     * @param table  table to append
     * @param result datafiles created by the {@link TaskWriter}
     */
    private void appendDataFiles(Table table, WriteResult result) {
        final AppendFiles appender = table.newAppend();
        Arrays.stream(result.dataFiles()).forEach(appender::appendFile);

        appender.commit();
    }

    /**
     * Determines the write file format from the requested value and the table configuration.
     *
     * @param tableProperties table properties
     * @param fileFormat      requested file format from the processor
     * @return file format to use
     */
    private FileFormat getFileFormat(Map<String, String> tableProperties, String fileFormat) {
        final String fileFormatName = fileFormat != null ? fileFormat : tableProperties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.valueOf(fileFormatName.toUpperCase(Locale.ENGLISH));
    }

    /**
     * Deletes the completed data files that have not been committed to the table yet.
     *
     * @param dataFiles files created by the task writer
     * @param table     table
     */
    void abort(DataFile[] dataFiles, Table table) {
        Tasks.foreach(dataFiles)
                .throwFailureWhenFinished()
                .noRetry()
                .run(file -> table.io().deleteFile(file.path().toString()));
    }

}
