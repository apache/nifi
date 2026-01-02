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
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.iceberg.record.DelegatedRecord;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.services.iceberg.IcebergCatalog;
import org.apache.nifi.services.iceberg.IcebergRowWriter;
import org.apache.nifi.services.iceberg.IcebergWriter;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"iceberg", "analytics", "polaris", "s3"})
@CapabilityDescription("Store records in Iceberg Table using configurable Catalog for managing namespaces and tables.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutIcebergRecord extends AbstractProcessor {

    static final PropertyDescriptor ICEBERG_CATALOG = new PropertyDescriptor.Builder()
            .name("Iceberg Catalog")
            .description("Provider Service for Iceberg Catalog")
            .required(true)
            .identifiesControllerService(IcebergCatalog.class)
            .build();

    static final PropertyDescriptor ICEBERG_WRITER = new PropertyDescriptor.Builder()
            .name("Iceberg Writer")
            .description("Provider Service for Iceberg Row Writers responsible for producing formatted Iceberg Data Files")
            .required(true)
            .identifiesControllerService(IcebergWriter.class)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Record Reader for incoming FlowFiles")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("Namespace")
            .description("Iceberg Namespace containing Tables")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("Iceberg Table Name")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles transferred to Iceberg")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles not transferred to Iceberg")
            .build();

    static final String RECORDS_PROCESSED_COUNTER = "Records Processed";

    static final String DATA_FILES_PROCESSED_COUNTER = "Data Files Processed";

    private static final List<PropertyDescriptor> properties = List.of(
            ICEBERG_CATALOG,
            ICEBERG_WRITER,
            RECORD_READER,
            NAMESPACE,
            TABLE_NAME
    );

    private static final Set<Relationship> relationships = Set.of(SUCCESS, FAILURE);

    private static final long MAXIMUM_BYTES = 536870912;

    private final Clock clock = Clock.systemDefaultZone();

    private volatile Catalog catalog;

    private volatile IcebergWriter icebergWriter;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final IcebergCatalog icebergCatalog = context.getProperty(ICEBERG_CATALOG).asControllerService(IcebergCatalog.class);
        catalog = icebergCatalog.getCatalog();
        icebergWriter = context.getProperty(ICEBERG_WRITER).asControllerService(IcebergWriter.class);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final TableIdentifierFlowFileFilter flowFileFilter = new TableIdentifierFlowFileFilter(context, MAXIMUM_BYTES);
        final List<FlowFile> flowFiles = session.get(flowFileFilter);
        if (flowFiles.isEmpty()) {
            return;
        }

        final TableIdentifier tableIdentifier = flowFileFilter.getTableIdentifier();
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        processFlowFiles(session, flowFiles, tableIdentifier, recordReaderFactory);
    }

    private void processFlowFiles(final ProcessSession session, final List<FlowFile> flowFiles, final TableIdentifier tableIdentifier, final RecordReaderFactory recordReaderFactory) {
        final long started = clock.millis();

        final AtomicReference<Relationship> relationship = new AtomicReference<>(SUCCESS);

        final Table table = getTable(tableIdentifier);
        final Schema schema = table.schema();
        final Types.StructType struct = schema.asStruct();
        final IcebergRowWriter rowWriter = icebergWriter.getRowWriter(table);

        for (final FlowFile flowFile : flowFiles) {
            try (
                    InputStream inputStream = session.read(flowFile);
                    RecordReader recordReader = recordReaderFactory.createRecordReader(flowFile, inputStream, getLogger())
            ) {
                final AtomicLong recordsProcessed = new AtomicLong();
                try {
                    writeRecords(recordReader, rowWriter, struct, recordsProcessed);
                    session.adjustCounter(RECORDS_PROCESSED_COUNTER, recordsProcessed.get(), false);
                } catch (final Exception e) {
                    getLogger().error("Write Rows to Table [{}] failed {}", tableIdentifier, flowFile, e);
                    abortWriter(rowWriter, tableIdentifier);
                    relationship.set(FAILURE);
                }
            } catch (final Exception e) {
                getLogger().error("Processing Records for Table [{}] failed {}", tableIdentifier, flowFile, e);
                abortWriter(rowWriter, tableIdentifier);
                relationship.set(FAILURE);
            }
        }

        if (SUCCESS.equals(relationship.get())) {
            try {
                final DataFile[] dataFiles = rowWriter.dataFiles();
                appendDataFiles(table, dataFiles);
                session.adjustCounter(DATA_FILES_PROCESSED_COUNTER, dataFiles.length, false);
            } catch (final Exception e) {
                getLogger().error("Appending Data Files to Table [{}] failed", tableIdentifier, e);
                relationship.set(FAILURE);
            }
        }

        try {
            rowWriter.close();
        } catch (final IOException e) {
            getLogger().warn("Failed to close Row Writer for Table [{}]", tableIdentifier, e);
        }

        if (SUCCESS.equals(relationship.get())) {
            final long elapsed = clock.millis() - started;
            final String transitUri = table.location();
            for (final FlowFile flowFile : flowFiles) {
                session.getProvenanceReporter().send(flowFile, transitUri, elapsed);
            }
        }

        session.transfer(flowFiles, relationship.get());
    }

    private Table getTable(final TableIdentifier tableIdentifier) {
        final Table table;

        if (catalog.tableExists(tableIdentifier)) {
            table = catalog.loadTable(tableIdentifier);
        } else {
            throw new IllegalStateException("Table [%s] not found in Catalog".formatted(tableIdentifier));
        }

        return table;
    }

    private void writeRecords(
            final RecordReader recordReader,
            final IcebergRowWriter rowWriter,
            final Types.StructType struct,
            final AtomicLong recordsProcessed
    ) throws IOException, MalformedRecordException {
        org.apache.nifi.serialization.record.Record inputRecord = recordReader.nextRecord();
        while (inputRecord != null) {
            final DelegatedRecord delegatedRecord = new DelegatedRecord(inputRecord, struct);
            // Write Records to storage based on Iceberg Table configuration
            rowWriter.write(delegatedRecord);
            inputRecord = recordReader.nextRecord();
            recordsProcessed.incrementAndGet();
        }
    }

    private void appendDataFiles(final Table table, final DataFile[] dataFiles) {
        final AppendFiles appendFiles = table.newAppend();
        for (final DataFile dataFile : dataFiles) {
            appendFiles.appendFile(dataFile);
        }

        appendFiles.commit();
    }

    private void abortWriter(final IcebergRowWriter rowWriter, final TableIdentifier tableIdentifier) {
        try {
            rowWriter.abort();
        } catch (final IOException e) {
            getLogger().warn("Abort Writing to Table [{}] failed", tableIdentifier, e);
        }
    }
}
