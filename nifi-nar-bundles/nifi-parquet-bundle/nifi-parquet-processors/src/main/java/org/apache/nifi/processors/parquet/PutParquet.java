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
package org.apache.nifi.processors.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.hadoop.AbstractPutHDFSRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.processors.parquet.record.AvroParquetHDFSRecordWriter;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "parquet", "hadoop", "HDFS", "filesystem", "restricted", "record"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records " +
        "to a Parquet file. The schema for the Parquet file must be provided in the processor properties. This processor will " +
        "first write a temporary dot file and upon successfully writing every record to the dot file, it will rename the " +
        "dot file to it's final name. If the dot file cannot be renamed, the rename operation will be attempted up to 10 times, and " +
        "if still not successful, the dot file will be deleted and the flow file will be routed to failure. " +
        " If any error occurs while reading records from the input, or writing records to the output, " +
        "the entire dot file will be removed and the flow file will be routed to failure or retry, depending on the error.")
@ReadsAttribute(attribute = "filename", description = "The name of the file to write comes from the value of this attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "The absolute path to the file is stored in this attribute."),
        @WritesAttribute(attribute = "record.count", description = "The number of records written to the Parquet file")
})
@Restricted("Provides operator the ability to write to any file that NiFi has access to in HDFS or the local filesystem.")
public class PutParquet extends AbstractPutHDFSRecord {

    public static final PropertyDescriptor ROW_GROUP_SIZE = new PropertyDescriptor.Builder()
            .name("row-group-size")
            .displayName("Row Group Size")
            .description("The row group size used by the Parquet writer. " +
                    "The value is specified in the format of <Data Size> <Data Unit> where Data Unit is one of B, KB, MB, GB, TB.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("page-size")
            .displayName("Page Size")
            .description("The page size used by the Parquet writer. " +
                    "The value is specified in the format of <Data Size> <Data Unit> where Data Unit is one of B, KB, MB, GB, TB.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor DICTIONARY_PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("dictionary-page-size")
            .displayName("Dictionary Page Size")
            .description("The dictionary page size used by the Parquet writer. " +
                    "The value is specified in the format of <Data Size> <Data Unit> where Data Unit is one of B, KB, MB, GB, TB.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor MAX_PADDING_SIZE = new PropertyDescriptor.Builder()
            .name("max-padding-size")
            .displayName("Max Padding Size")
            .description("The maximum amount of padding that will be used to align row groups with blocks in the " +
                    "underlying filesystem. If the underlying filesystem is not a block filesystem like HDFS, this has no effect. " +
                    "The value is specified in the format of <Data Size> <Data Unit> where Data Unit is one of B, KB, MB, GB, TB.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor ENABLE_DICTIONARY_ENCODING = new PropertyDescriptor.Builder()
            .name("enable-dictionary-encoding")
            .displayName("Enable Dictionary Encoding")
            .description("Specifies whether dictionary encoding should be enabled for the Parquet writer")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor ENABLE_VALIDATION = new PropertyDescriptor.Builder()
            .name("enable-validation")
            .displayName("Enable Validation")
            .description("Specifies whether validation should be enabled for the Parquet writer")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor WRITER_VERSION = new PropertyDescriptor.Builder()
            .name("writer-version")
            .displayName("Writer Version")
            .description("Specifies the version used by Parquet writer")
            .allowableValues(ParquetProperties.WriterVersion.values())
            .build();

    public static final PropertyDescriptor REMOVE_CRC_FILES = new PropertyDescriptor.Builder()
            .name("remove-crc-files")
            .displayName("Remove CRC Files")
            .description("Specifies whether the corresponding CRC file should be deleted upon successfully writing a Parquet file")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final List<AllowableValue> COMPRESSION_TYPES;
    static {
        final List<AllowableValue> compressionTypes = new ArrayList<>();
        for (CompressionCodecName compressionCodecName : CompressionCodecName.values()) {
            final String name = compressionCodecName.name();
            compressionTypes.add(new AllowableValue(name, name));
        }
        COMPRESSION_TYPES = Collections.unmodifiableList(compressionTypes);
    }

    @Override
    public List<AllowableValue> getCompressionTypes(final ProcessorInitializationContext context) {
        return COMPRESSION_TYPES;
    }

    @Override
    public String getDefaultCompressionType(final ProcessorInitializationContext context) {
        return CompressionCodecName.UNCOMPRESSED.name();
    }

    @Override
    public List<PropertyDescriptor> getAdditionalProperties() {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ROW_GROUP_SIZE);
        props.add(PAGE_SIZE);
        props.add(DICTIONARY_PAGE_SIZE);
        props.add(MAX_PADDING_SIZE);
        props.add(ENABLE_DICTIONARY_ENCODING);
        props.add(ENABLE_VALIDATION);
        props.add(WRITER_VERSION);
        props.add(REMOVE_CRC_FILES);
        return Collections.unmodifiableList(props);
    }

    @Override
    public HDFSRecordWriter createHDFSRecordWriter(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path, final RecordSchema schema)
            throws IOException, SchemaNotFoundException {

        final Schema avroSchema = AvroTypeUtil.extractAvroSchema(schema);

        final AvroParquetWriter.Builder<GenericRecord> parquetWriter = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(avroSchema);

        applyCommonConfig(parquetWriter, context, flowFile, conf);

        return new AvroParquetHDFSRecordWriter(parquetWriter.build(), avroSchema);
    }

    private void applyCommonConfig(final ParquetWriter.Builder<?, ?> builder, final ProcessContext context, final FlowFile flowFile, final Configuration conf) {
        builder.withConf(conf);

        // Required properties

        final boolean overwrite = context.getProperty(OVERWRITE).asBoolean();
        final ParquetFileWriter.Mode mode = overwrite ? ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE;
        builder.withWriteMode(mode);

        final PropertyDescriptor compressionTypeDescriptor = getPropertyDescriptor(COMPRESSION_TYPE.getName());
        final String compressionTypeValue = context.getProperty(compressionTypeDescriptor).getValue();

        final CompressionCodecName codecName = CompressionCodecName.valueOf(compressionTypeValue);
        builder.withCompressionCodec(codecName);

        // Optional properties

        if (context.getProperty(ROW_GROUP_SIZE).isSet()){
            try {
                final Double rowGroupSize = context.getProperty(ROW_GROUP_SIZE).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B);
                if (rowGroupSize != null) {
                    builder.withRowGroupSize(rowGroupSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + ROW_GROUP_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(PAGE_SIZE).isSet()) {
            try {
                final Double pageSize = context.getProperty(PAGE_SIZE).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B);
                if (pageSize != null) {
                    builder.withPageSize(pageSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + PAGE_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(DICTIONARY_PAGE_SIZE).isSet()) {
            try {
                final Double dictionaryPageSize = context.getProperty(DICTIONARY_PAGE_SIZE).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B);
                if (dictionaryPageSize != null) {
                    builder.withDictionaryPageSize(dictionaryPageSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + DICTIONARY_PAGE_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(MAX_PADDING_SIZE).isSet()) {
            try {
                final Double maxPaddingSize = context.getProperty(MAX_PADDING_SIZE).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B);
                if (maxPaddingSize != null) {
                    builder.withMaxPaddingSize(maxPaddingSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + MAX_PADDING_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(ENABLE_DICTIONARY_ENCODING).isSet()) {
            final boolean enableDictionaryEncoding = context.getProperty(ENABLE_DICTIONARY_ENCODING).asBoolean();
            builder.withDictionaryEncoding(enableDictionaryEncoding);
        }

        if (context.getProperty(ENABLE_VALIDATION).isSet()) {
            final boolean enableValidation = context.getProperty(ENABLE_VALIDATION).asBoolean();
            builder.withValidation(enableValidation);
        }

        if (context.getProperty(WRITER_VERSION).isSet()) {
            final String writerVersionValue = context.getProperty(WRITER_VERSION).getValue();
            builder.withWriterVersion(ParquetProperties.WriterVersion.valueOf(writerVersionValue));
        }
    }

    @Override
    protected FlowFile postProcess(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Path destFile) {
        final boolean removeCRCFiles = context.getProperty(REMOVE_CRC_FILES).asBoolean();
        if (removeCRCFiles) {
            final String filename = destFile.getName();
            final String hdfsPath = destFile.getParent().toString();

            final Path crcFile = new Path(hdfsPath, "." + filename + ".crc");
            deleteQuietly(getFileSystem(), crcFile);
        }

        return flowFile;
    }
}
