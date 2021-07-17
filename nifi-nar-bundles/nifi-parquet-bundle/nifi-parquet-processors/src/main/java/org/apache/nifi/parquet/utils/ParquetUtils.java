/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.parquet.PutParquet;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ParquetUtils {

    public static final PropertyDescriptor ROW_GROUP_SIZE = new PropertyDescriptor.Builder()
            .name("row-group-size")
            .displayName("Row Group Size")
            .description("The row group size used by the Parquet writer. " +
                    "The value is specified in the format of <Data Size> <Data Unit> where Data Unit is one of B, KB, MB, GB, TB.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("page-size")
            .displayName("Page Size")
            .description("The page size used by the Parquet writer. " +
                    "The value is specified in the format of <Data Size> <Data Unit> where Data Unit is one of B, KB, MB, GB, TB.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DICTIONARY_PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("dictionary-page-size")
            .displayName("Dictionary Page Size")
            .description("The dictionary page size used by the Parquet writer. " +
                    "The value is specified in the format of <Data Size> <Data Unit> where Data Unit is one of B, KB, MB, GB, TB.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_PADDING_SIZE = new PropertyDescriptor.Builder()
            .name("max-padding-size")
            .displayName("Max Padding Size")
            .description("The maximum amount of padding that will be used to align row groups with blocks in the " +
                    "underlying filesystem. If the underlying filesystem is not a block filesystem like HDFS, this has no effect. " +
                    "The value is specified in the format of <Data Size> <Data Unit> where Data Unit is one of B, KB, MB, GB, TB.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
            .allowableValues(org.apache.parquet.column.ParquetProperties.WriterVersion.values())
            .build();

    public static final PropertyDescriptor AVRO_READ_COMPATIBILITY = new PropertyDescriptor.Builder()
            .name("avro-read-compatibility")
            .displayName("Avro Read Compatibility")
            .description("Specifies the value for '" + AvroReadSupport.AVRO_COMPATIBILITY + "' in the underlying Parquet library")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor AVRO_ADD_LIST_ELEMENT_RECORDS = new PropertyDescriptor.Builder()
            .name("avro-add-list-element-records")
            .displayName("Avro Add List Element Records")
            .description("Specifies the value for '" + AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS + "' in the underlying Parquet library")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final PropertyDescriptor AVRO_WRITE_OLD_LIST_STRUCTURE = new PropertyDescriptor.Builder()
            .name("avro-write-old-list-structure")
            .displayName("Avro Write Old List Structure")
            .description("Specifies the value for '" + AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE + "' in the underlying Parquet library")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    public static final List<AllowableValue> COMPRESSION_TYPES = getCompressionTypes();

    private static List<AllowableValue> getCompressionTypes() {
        final List<AllowableValue> compressionTypes = new ArrayList<>();
        for (CompressionCodecName compressionCodecName : CompressionCodecName.values()) {
            final String name = compressionCodecName.name();
            compressionTypes.add(new AllowableValue(name, name));
        }
        return  Collections.unmodifiableList(compressionTypes);
    }

    // NOTE: This needs to be named the same as the compression property in AbstractPutHDFSRecord
    public static final String COMPRESSION_TYPE_PROP_NAME = "compression-type";

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name(COMPRESSION_TYPE_PROP_NAME)
            .displayName("Compression Type")
            .description("The type of compression for the file being written.")
            .allowableValues(COMPRESSION_TYPES.toArray(new AllowableValue[0]))
            .defaultValue(COMPRESSION_TYPES.get(0).getValue())
            .required(true)
            .build();

    /**
     * Creates a ParquetConfig instance from the given PropertyContext.
     *
     * @param context the PropertyContext from a component
     * @param variables an optional set of variables to evaluate EL against, may be null
     * @return the ParquetConfig
     */
    public static ParquetConfig createParquetConfig(final PropertyContext context, final Map<String, String> variables) {
        final ParquetConfig parquetConfig = new ParquetConfig();

        // Required properties
        boolean overwrite = true;
        if(context.getProperty(PutParquet.OVERWRITE).isSet()) {
            overwrite = context.getProperty(PutParquet.OVERWRITE).asBoolean();
        }

        final ParquetFileWriter.Mode mode = overwrite ? ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE;
        parquetConfig.setWriterMode(mode);

        if(context.getProperty(ParquetUtils.COMPRESSION_TYPE).isSet()) {
            final String compressionTypeValue = context.getProperty(ParquetUtils.COMPRESSION_TYPE).getValue();
            final CompressionCodecName codecName = CompressionCodecName.valueOf(compressionTypeValue);
            parquetConfig.setCompressionCodec(codecName);
        }

        // Optional properties

        if (context.getProperty(ROW_GROUP_SIZE).isSet()){
            try {
                final Double rowGroupSize = context.getProperty(ROW_GROUP_SIZE).evaluateAttributeExpressions(variables).asDataSize(DataUnit.B);
                if (rowGroupSize != null) {
                    parquetConfig.setRowGroupSize(rowGroupSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + ROW_GROUP_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(PAGE_SIZE).isSet()) {
            try {
                final Double pageSize = context.getProperty(PAGE_SIZE).evaluateAttributeExpressions(variables).asDataSize(DataUnit.B);
                if (pageSize != null) {
                    parquetConfig.setPageSize(pageSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + PAGE_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(DICTIONARY_PAGE_SIZE).isSet()) {
            try {
                final Double dictionaryPageSize = context.getProperty(DICTIONARY_PAGE_SIZE).evaluateAttributeExpressions(variables).asDataSize(DataUnit.B);
                if (dictionaryPageSize != null) {
                    parquetConfig.setDictionaryPageSize(dictionaryPageSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + DICTIONARY_PAGE_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(MAX_PADDING_SIZE).isSet()) {
            try {
                final Double maxPaddingSize = context.getProperty(MAX_PADDING_SIZE).evaluateAttributeExpressions(variables).asDataSize(DataUnit.B);
                if (maxPaddingSize != null) {
                    parquetConfig.setMaxPaddingSize(maxPaddingSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + MAX_PADDING_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(ENABLE_DICTIONARY_ENCODING).isSet()) {
            final boolean enableDictionaryEncoding = context.getProperty(ENABLE_DICTIONARY_ENCODING).asBoolean();
            parquetConfig.setEnableDictionaryEncoding(enableDictionaryEncoding);
        }

        if (context.getProperty(ENABLE_VALIDATION).isSet()) {
            final boolean enableValidation = context.getProperty(ENABLE_VALIDATION).asBoolean();
            parquetConfig.setEnableValidation(enableValidation);
        }

        if (context.getProperty(WRITER_VERSION).isSet()) {
            final String writerVersionValue = context.getProperty(WRITER_VERSION).getValue();
            parquetConfig.setWriterVersion(ParquetProperties.WriterVersion.valueOf(writerVersionValue));
        }

        if (context.getProperty(AVRO_READ_COMPATIBILITY).isSet()) {
            final boolean avroReadCompatibility = context.getProperty(AVRO_READ_COMPATIBILITY).asBoolean();
            parquetConfig.setAvroReadCompatibility(avroReadCompatibility);
        }

        if (context.getProperty(AVRO_ADD_LIST_ELEMENT_RECORDS).isSet()) {
            final boolean avroAddListElementRecords = context.getProperty(AVRO_ADD_LIST_ELEMENT_RECORDS).asBoolean();
            parquetConfig.setAvroAddListElementRecords(avroAddListElementRecords);
        }

        if (context.getProperty(AVRO_WRITE_OLD_LIST_STRUCTURE).isSet()) {
            final boolean avroWriteOldListStructure = context.getProperty(AVRO_WRITE_OLD_LIST_STRUCTURE).asBoolean();
            parquetConfig.setAvroWriteOldListStructure(avroWriteOldListStructure);
        }

        return parquetConfig;
    }

    public static void applyCommonConfig(final ParquetWriter.Builder<?, ?> builder, final Configuration conf,
                                         final ParquetConfig parquetConfig) {
        builder.withConf(conf);
        builder.withCompressionCodec(parquetConfig.getCompressionCodec());

        // Optional properties

        if (parquetConfig.getRowGroupSize() != null){
            builder.withRowGroupSize(parquetConfig.getRowGroupSize());
        }

        if (parquetConfig.getPageSize() != null) {
            builder.withPageSize(parquetConfig.getPageSize());
        }

        if (parquetConfig.getDictionaryPageSize() != null) {
            builder.withDictionaryPageSize(parquetConfig.getDictionaryPageSize());
        }

        if (parquetConfig.getMaxPaddingSize() != null) {
            builder.withMaxPaddingSize(parquetConfig.getMaxPaddingSize());
        }

        if (parquetConfig.getEnableDictionaryEncoding() != null) {
            builder.withDictionaryEncoding(parquetConfig.getEnableDictionaryEncoding());
        }

        if (parquetConfig.getEnableValidation() != null) {
            builder.withValidation(parquetConfig.getEnableValidation());
        }

        if (parquetConfig.getWriterVersion() != null) {
            builder.withWriterVersion(parquetConfig.getWriterVersion());
        }

        if (parquetConfig.getWriterMode() != null) {
            builder.withWriteMode(parquetConfig.getWriterMode());
        }

        applyCommonConfig(conf, parquetConfig);
    }

    public static void applyCommonConfig(Configuration conf, ParquetConfig parquetConfig) {
        if (parquetConfig.getAvroReadCompatibility() != null) {
            conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY,
                    parquetConfig.getAvroReadCompatibility().booleanValue());
        }

        if (parquetConfig.getAvroAddListElementRecords() != null) {
            conf.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS,
                    parquetConfig.getAvroAddListElementRecords().booleanValue());
        }

        if (parquetConfig.getAvroWriteOldListStructure() != null) {
            conf.setBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE,
                    parquetConfig.getAvroWriteOldListStructure().booleanValue());
        }
        conf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
        if (parquetConfig.getInt96Fields() != null) {
            conf.setStrings("parquet.avro.writeFixedAsInt96",
                parquetConfig.getInt96Fields());
        }
    }
}
