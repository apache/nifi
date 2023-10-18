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

import java.io.IOException;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.parquet.filter.OffsetRecordFilter;
import org.apache.nifi.parquet.hadoop.AvroParquetHDFSRecordReader;
import org.apache.nifi.parquet.utils.ParquetAttribute;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.hadoop.AbstractFetchHDFSRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordReader;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"parquet", "hadoop", "HDFS", "get", "ingest", "fetch", "source", "record"})
@CapabilityDescription("Reads from a given Parquet file and writes records to the content of the flow file using " +
        "the selected record writer. The original Parquet file will remain unchanged, and the content of the flow file " +
        "will be replaced with records of the selected type. This processor can be used with ListHDFS or ListFile to obtain " +
        "a listing of files to fetch.")
@WritesAttributes({
        @WritesAttribute(attribute="fetch.failure.reason", description="When a FlowFile is routed to 'failure', this attribute is added " +
                "indicating why the file could not be fetched from the given filesystem."),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the resulting flow file"),
        @WritesAttribute(attribute = "hadoop.file.url", description = "The hadoop url for the file is stored in this attribute.")
})
@ReadsAttributes({
        @ReadsAttribute(
                attribute = ParquetAttribute.RECORD_OFFSET,
                description = "Gets the index of first record in the input."
        ),
        @ReadsAttribute(
                attribute = ParquetAttribute.RECORD_COUNT,
                description = "Gets the number of records in the input."
        )
})
@SeeAlso({PutParquet.class})
@Restricted(restrictions = {
    @Restriction(
        requiredPermission = RequiredPermission.READ_DISTRIBUTED_FILESYSTEM,
        explanation = "Provides operator the ability to retrieve any file that NiFi has access to in HDFS or the local filesystem.")
})
public class FetchParquet extends AbstractFetchHDFSRecord {

    @Override
    public HDFSRecordReader createHDFSRecordReader(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path) throws IOException {
        final Long offset = Optional.ofNullable(flowFile.getAttribute(ParquetAttribute.RECORD_OFFSET))
                .map(Long::parseLong)
                .orElse(null);

        final Long count = Optional.ofNullable(flowFile.getAttribute(ParquetAttribute.RECORD_COUNT))
                .map(Long::parseLong)
                .orElse(null);

        final long fileStartOffset = Optional.ofNullable(flowFile.getAttribute(ParquetAttribute.FILE_RANGE_START_OFFSET))
                .map(Long::parseLong)
                .orElse(0L);
        final long fileEndOffset = Optional.ofNullable(flowFile.getAttribute(ParquetAttribute.FILE_RANGE_END_OFFSET))
                .map(Long::parseLong)
                .orElse(Long.MAX_VALUE);

        final InputFile inputFile = HadoopInputFile.fromPath(path, conf);
        final ParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.<GenericRecord>builder(inputFile)
                .withConf(conf)
                .withFileRange(fileStartOffset, fileEndOffset);

        if (offset != null) {
            readerBuilder.withFilter(FilterCompat.get(OffsetRecordFilter.offset(offset)));
        }

        return new AvroParquetHDFSRecordReader(readerBuilder.build(), count);
    }

}
