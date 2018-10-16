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
package org.apache.nifi.processors.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.parquet.PutParquet;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    public static List<AllowableValue> COMPRESSION_TYPES = getCompressionTypes();

    public static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("compression-type")
            .displayName("Compression Type")
            .description("The type of compression for the file being written.")
            .allowableValues(COMPRESSION_TYPES.toArray(new AllowableValue[0]))
            .defaultValue(COMPRESSION_TYPES.get(0).getValue())
            .required(true)
            .build();

    public static List<AllowableValue> getCompressionTypes() {
        final List<AllowableValue> compressionTypes = new ArrayList<>();
        for (CompressionCodecName compressionCodecName : CompressionCodecName.values()) {
            final String name = compressionCodecName.name();
            compressionTypes.add(new AllowableValue(name, name));
        }
        return  Collections.unmodifiableList(compressionTypes);
    }

    public static void applyCommonConfig(final ParquetWriter.Builder<?, ?> builder,
                                         final ProcessContext context,
                                         final FlowFile flowFile,
                                         final Configuration conf,
                                         final AbstractProcessor abstractProcessor) {
        builder.withConf(conf);

        // Required properties
        boolean overwrite = true;
        if(context.getProperty(PutParquet.OVERWRITE).isSet())
            overwrite = context.getProperty(PutParquet.OVERWRITE).asBoolean();

        final ParquetFileWriter.Mode mode = overwrite ? ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE;
        builder.withWriteMode(mode);

        final PropertyDescriptor compressionTypeDescriptor = abstractProcessor.getPropertyDescriptor(COMPRESSION_TYPE.getName());

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
}
