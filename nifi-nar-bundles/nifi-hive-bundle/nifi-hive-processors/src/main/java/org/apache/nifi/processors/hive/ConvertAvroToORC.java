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
package org.apache.nifi.processors.hive;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFlowFileWriter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.hive.HiveJdbcCommon;
import org.apache.nifi.util.hive.HiveUtils;
import org.apache.hadoop.hive.ql.io.orc.NiFiOrcUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The ConvertAvroToORC processor takes an Avro-formatted flow file as input and converts it into ORC format.
 */
@SideEffectFree
@SupportsBatching
@Tags({"avro", "orc", "hive", "convert"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts an Avro record into ORC file format. This processor provides a direct mapping of an Avro record to an ORC record, such "
        + "that the resulting ORC file will have the same hierarchical structure as the Avro document. If an incoming FlowFile contains a stream of "
        + "multiple Avro records, the resultant FlowFile will contain a ORC file containing all of the Avro records.  If an incoming FlowFile does "
        + "not contain any records, an empty ORC file is the output. NOTE: Many Avro datatypes (collections, primitives, and unions of primitives, e.g.) can "
        + "be converted to ORC, but unions of collections and other complex datatypes may not be able to be converted to ORC.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime type to application/octet-stream"),
        @WritesAttribute(attribute = "filename", description = "Sets the filename to the existing filename with the extension replaced by / added to by .orc"),
        @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the ORC file."),
        @WritesAttribute(attribute = "hive.ddl", description = "Creates a partial Hive DDL statement for creating a table in Hive from this ORC file. "
                + "This can be used in ReplaceText for setting the content to the DDL. To make it valid DDL, add \"LOCATION '<path_to_orc_file_in_hdfs>'\", where "
                + "the path is the directory that contains this ORC file on HDFS. For example, ConvertAvroToORC can send flow files to a PutHDFS processor to send the file to "
                + "HDFS, then to a ReplaceText to set the content to this DDL (plus the LOCATION clause as described), then to PutHiveQL processor to create the table "
                + "if it doesn't exist.")
})
public class ConvertAvroToORC extends AbstractProcessor {

    // Attributes
    public static final String ORC_MIME_TYPE = "application/octet-stream";
    public static final String HIVE_DDL_ATTRIBUTE = "hive.ddl";
    public static final String RECORD_COUNT_ATTRIBUTE = "record.count";


    // Properties
    public static final PropertyDescriptor ORC_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("orc-config-resources")
            .displayName("ORC Configuration Resources")
            .description("A file or comma separated list of files which contains the ORC configuration (hive-site.xml, e.g.). Without this, Hadoop "
                    + "will search the classpath for a 'hive-site.xml' file or will revert to a default configuration. Please see the ORC documentation for more details.")
            .required(false).addValidator(HiveUtils.createMultipleFilesExistValidator()).build();

    public static final PropertyDescriptor STRIPE_SIZE = new PropertyDescriptor.Builder()
            .name("orc-stripe-size")
            .displayName("Stripe Size")
            .description("The size of the memory buffer (in bytes) for writing stripes to an ORC file")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("64 MB")
            .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("orc-buffer-size")
            .displayName("Buffer Size")
            .description("The maximum size of the memory buffers (in bytes) used for compressing and storing a stripe in memory. This is a hint to the ORC writer, "
                    + "which may choose to use a smaller buffer size based on stripe size and number of columns for efficient stripe writing and memory utilization.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("10 KB")
            .build();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("orc-compression-type")
            .displayName("Compression Type")
            .required(true)
            .allowableValues("NONE", "ZLIB", "SNAPPY", "LZO")
            .defaultValue("NONE")
            .build();

    public static final PropertyDescriptor HIVE_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("orc-hive-table-name")
            .displayName("Hive Table Name")
            .description("An optional table name to insert into the hive.ddl attribute. The generated DDL can be used by "
                    + "a PutHiveQL processor (presumably after a PutHDFS processor) to create a table backed by the converted ORC file. "
                    + "If this property is not provided, the full name (including namespace) of the incoming Avro record will be normalized "
                    + "and used as the table name.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    // Relationships
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been converted to ORC format.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be parsed as Avro or cannot be converted to ORC for any reason")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    private volatile Configuration orcConfig;

    /*
     * Will ensure that the list of property descriptors is built only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(ORC_CONFIGURATION_RESOURCES);
        _propertyDescriptors.add(STRIPE_SIZE);
        _propertyDescriptors.add(BUFFER_SIZE);
        _propertyDescriptors.add(COMPRESSION_TYPE);
        _propertyDescriptors.add(HIVE_TABLE_NAME);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        boolean confFileProvided = context.getProperty(ORC_CONFIGURATION_RESOURCES).isSet();
        if (confFileProvided) {
            final String configFiles = context.getProperty(ORC_CONFIGURATION_RESOURCES).getValue();
            orcConfig = HiveJdbcCommon.getConfigurationFromFiles(configFiles);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            long startTime = System.currentTimeMillis();
            final long stripeSize = context.getProperty(STRIPE_SIZE).asDataSize(DataUnit.B).longValue();
            final int bufferSize = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
            final CompressionKind compressionType = CompressionKind.valueOf(context.getProperty(COMPRESSION_TYPE).getValue());
            final AtomicReference<Schema> hiveAvroSchema = new AtomicReference<>(null);
            final AtomicInteger totalRecordCount = new AtomicInteger(0);
            final String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());
            flowFile = session.write(flowFile, (rawIn, rawOut) -> {
                try (final InputStream in = new BufferedInputStream(rawIn);
                     final OutputStream out = new BufferedOutputStream(rawOut);
                     final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<>())) {

                    // Create ORC schema from Avro schema
                    Schema avroSchema = reader.getSchema();

                    TypeInfo orcSchema = NiFiOrcUtils.getOrcField(avroSchema);

                    if (orcConfig == null) {
                        orcConfig = new Configuration();
                    }

                    OrcFlowFileWriter orcWriter = NiFiOrcUtils.createWriter(
                            out,
                            new Path(fileName),
                            orcConfig,
                            orcSchema,
                            stripeSize,
                            compressionType,
                            bufferSize);
                    try {

                        int recordCount = 0;
                        GenericRecord currRecord = null;
                        while (reader.hasNext()) {
                            currRecord = reader.next(currRecord);
                            List<Schema.Field> fields = currRecord.getSchema().getFields();
                            if (fields != null) {
                                Object[] row = new Object[fields.size()];
                                for (int i = 0; i < fields.size(); i++) {
                                    Schema.Field field = fields.get(i);
                                    Schema fieldSchema = field.schema();
                                    Object o = currRecord.get(field.name());
                                    try {
                                        row[i] = NiFiOrcUtils.convertToORCObject(NiFiOrcUtils.getOrcField(fieldSchema), o);
                                    } catch (ArrayIndexOutOfBoundsException aioobe) {
                                        getLogger().error("Index out of bounds at record {} for column {}, type {}, and object {}",
                                                new Object[]{recordCount, i, fieldSchema.getType().getName(), o.toString()},
                                                aioobe);
                                        throw new IOException(aioobe);
                                    }
                                }
                                orcWriter.addRow(NiFiOrcUtils.createOrcStruct(orcSchema, row));
                                recordCount++;
                            }
                        }
                        hiveAvroSchema.set(avroSchema);
                        totalRecordCount.set(recordCount);
                    } finally {
                        // finished writing this record, close the writer (which will flush to the flow file)
                        orcWriter.close();
                    }
                }
            });

            final String hiveTableName = context.getProperty(HIVE_TABLE_NAME).isSet()
                    ? context.getProperty(HIVE_TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue()
                    : NiFiOrcUtils.normalizeHiveTableName(hiveAvroSchema.get().getFullName());
            String hiveDDL = NiFiOrcUtils.generateHiveDDL(hiveAvroSchema.get(), hiveTableName);
            // Add attributes and transfer to success
            flowFile = session.putAttribute(flowFile, RECORD_COUNT_ATTRIBUTE, Integer.toString(totalRecordCount.get()));
            flowFile = session.putAttribute(flowFile, HIVE_DDL_ATTRIBUTE, hiveDDL);
            StringBuilder newFilename = new StringBuilder();
            int extensionIndex = fileName.lastIndexOf(".");
            if (extensionIndex != -1) {
                newFilename.append(fileName.substring(0, extensionIndex));
            } else {
                newFilename.append(fileName);
            }
            newFilename.append(".orc");
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), ORC_MIME_TYPE);
            flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), newFilename.toString());
            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(flowFile, "Converted "+totalRecordCount.get()+" records", System.currentTimeMillis() - startTime);

        } catch (final ProcessException pe) {
            getLogger().error("Failed to convert {} from Avro to ORC due to {}; transferring to failure", new Object[]{flowFile, pe});
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
