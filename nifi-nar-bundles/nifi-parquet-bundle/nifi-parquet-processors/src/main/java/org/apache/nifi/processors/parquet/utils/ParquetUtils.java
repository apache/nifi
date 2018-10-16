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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.parquet.PutParquet;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetUtils {

    public static void applyCommonConfig(final ParquetWriter.Builder<?, ?> builder,
                                         final ProcessContext context,
                                         final FlowFile flowFile,
                                         final Configuration conf,
                                         final ParquetBuilderProperties parquetBuilderProperties,
                                         final AbstractProcessor abstractProcessor) {
        builder.withConf(conf);

        // Required properties
        boolean overwrite = true;
        if(context.getProperty(PutParquet.OVERWRITE).isSet())
            overwrite = context.getProperty(PutParquet.OVERWRITE).asBoolean();

        final ParquetFileWriter.Mode mode = overwrite ? ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE;
        builder.withWriteMode(mode);

        final PropertyDescriptor compressionTypeDescriptor = abstractProcessor.getPropertyDescriptor(parquetBuilderProperties.COMPRESSION_TYPE.getName());

        final String compressionTypeValue = context.getProperty(compressionTypeDescriptor).getValue();

        final CompressionCodecName codecName = CompressionCodecName.valueOf(compressionTypeValue);
        builder.withCompressionCodec(codecName);

        // Optional properties

        if (context.getProperty(parquetBuilderProperties.ROW_GROUP_SIZE).isSet()){
            try {
                final Double rowGroupSize = context.getProperty(parquetBuilderProperties.ROW_GROUP_SIZE).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B);
                if (rowGroupSize != null) {
                    builder.withRowGroupSize(rowGroupSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + parquetBuilderProperties.ROW_GROUP_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(parquetBuilderProperties.PAGE_SIZE).isSet()) {
            try {
                final Double pageSize = context.getProperty(parquetBuilderProperties.PAGE_SIZE).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B);
                if (pageSize != null) {
                    builder.withPageSize(pageSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + parquetBuilderProperties.PAGE_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(parquetBuilderProperties.DICTIONARY_PAGE_SIZE).isSet()) {
            try {
                final Double dictionaryPageSize = context.getProperty(parquetBuilderProperties.DICTIONARY_PAGE_SIZE).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B);
                if (dictionaryPageSize != null) {
                    builder.withDictionaryPageSize(dictionaryPageSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + parquetBuilderProperties.DICTIONARY_PAGE_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(parquetBuilderProperties.MAX_PADDING_SIZE).isSet()) {
            try {
                final Double maxPaddingSize = context.getProperty(parquetBuilderProperties.MAX_PADDING_SIZE).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B);
                if (maxPaddingSize != null) {
                    builder.withMaxPaddingSize(maxPaddingSize.intValue());
                }
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid data size for " + parquetBuilderProperties.MAX_PADDING_SIZE.getDisplayName(), e);
            }
        }

        if (context.getProperty(parquetBuilderProperties.ENABLE_DICTIONARY_ENCODING).isSet()) {
            final boolean enableDictionaryEncoding = context.getProperty(parquetBuilderProperties.ENABLE_DICTIONARY_ENCODING).asBoolean();
            builder.withDictionaryEncoding(enableDictionaryEncoding);
        }

        if (context.getProperty(parquetBuilderProperties.ENABLE_VALIDATION).isSet()) {
            final boolean enableValidation = context.getProperty(parquetBuilderProperties.ENABLE_VALIDATION).asBoolean();
            builder.withValidation(enableValidation);
        }

        if (context.getProperty(parquetBuilderProperties.WRITER_VERSION).isSet()) {
            final String writerVersionValue = context.getProperty(parquetBuilderProperties.WRITER_VERSION).getValue();
            builder.withWriterVersion(ParquetProperties.WriterVersion.valueOf(writerVersionValue));
        }
    }
}
