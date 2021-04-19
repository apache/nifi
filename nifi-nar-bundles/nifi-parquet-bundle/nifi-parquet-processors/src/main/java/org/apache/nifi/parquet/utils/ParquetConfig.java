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
package org.apache.nifi.parquet.utils;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetConfig {

    private Integer rowGroupSize;
    private Integer pageSize;
    private Integer dictionaryPageSize;
    private Integer maxPaddingSize;
    private Boolean enableDictionaryEncoding;
    private Boolean enableValidation;
    private Boolean avroReadCompatibility;
    private Boolean avroAddListElementRecords;
    private Boolean avroWriteOldListStructure;
    private ParquetProperties.WriterVersion writerVersion;
    private ParquetFileWriter.Mode writerMode;
    private CompressionCodecName compressionCodec;
    private String int96Fields;

    public Integer getRowGroupSize() {
        return rowGroupSize;
    }

    public void setRowGroupSize(Integer rowGroupSize) {
        this.rowGroupSize = rowGroupSize;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public Integer getDictionaryPageSize() {
        return dictionaryPageSize;
    }

    public void setDictionaryPageSize(Integer dictionaryPageSize) {
        this.dictionaryPageSize = dictionaryPageSize;
    }

    public Integer getMaxPaddingSize() {
        return maxPaddingSize;
    }

    public void setMaxPaddingSize(Integer maxPaddingSize) {
        this.maxPaddingSize = maxPaddingSize;
    }

    public Boolean getEnableDictionaryEncoding() {
        return enableDictionaryEncoding;
    }

    public void setEnableDictionaryEncoding(Boolean enableDictionaryEncoding) {
        this.enableDictionaryEncoding = enableDictionaryEncoding;
    }

    public Boolean getEnableValidation() {
        return enableValidation;
    }

    public void setEnableValidation(Boolean enableValidation) {
        this.enableValidation = enableValidation;
    }

    public Boolean getAvroReadCompatibility() {
        return avroReadCompatibility;
    }

    public void setAvroReadCompatibility(Boolean avroReadCompatibility) {
        this.avroReadCompatibility = avroReadCompatibility;
    }

    public Boolean getAvroAddListElementRecords() {
        return avroAddListElementRecords;
    }

    public void setAvroAddListElementRecords(Boolean avroAddListElementRecords) {
        this.avroAddListElementRecords = avroAddListElementRecords;
    }

    public Boolean getAvroWriteOldListStructure() {
        return avroWriteOldListStructure;
    }

    public void setAvroWriteOldListStructure(Boolean avroWriteOldListStructure) {
        this.avroWriteOldListStructure = avroWriteOldListStructure;
    }

    public ParquetProperties.WriterVersion getWriterVersion() {
        return writerVersion;
    }

    public void setWriterVersion(ParquetProperties.WriterVersion writerVersion) {
        this.writerVersion = writerVersion;
    }

    public ParquetFileWriter.Mode getWriterMode() {
        return writerMode;
    }

    public void setWriterMode(ParquetFileWriter.Mode writerMode) {
        this.writerMode = writerMode;
    }

    public CompressionCodecName getCompressionCodec() {
        return compressionCodec;
    }

    public void setCompressionCodec(CompressionCodecName compressionCodec) {
        this.compressionCodec = compressionCodec;
    }

    public String getInt96Fields() {
        return int96Fields;
    }

    public void setInt96Fields(String int96Fields) {
        this.int96Fields = int96Fields;
    }
}
