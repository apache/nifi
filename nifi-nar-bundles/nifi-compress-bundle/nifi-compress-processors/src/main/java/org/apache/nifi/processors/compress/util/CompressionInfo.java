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
package org.apache.nifi.processors.compress.util;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;

public enum CompressionInfo implements DescribedValue {

    DECOMPRESSION_FORMAT_NONE("no decompression", "Do not decompress the input content", ""),
    COMPRESSION_FORMAT_NONE("no compression", "Do not compress the output content", ""),
    COMPRESSION_FORMAT_ATTRIBUTE("use mime.type attribute", "Use the 'mime.type' attribute from the incoming FlowFile to determine the value", ""),
    COMPRESSION_FORMAT_GZIP("gzip", "GZIP", ".gz","application/gzip", "application/x-gzip"),
    COMPRESSION_FORMAT_DEFLATE("deflate", "Deflate", ".zlib","application/deflate", "application/x-deflate"),
    COMPRESSION_FORMAT_BZIP2("bzip2", "BZIP2", ".bz2","application/x-bzip2", "application/bzip2"),
    COMPRESSION_FORMAT_XZ_LZMA2("xz-lzma2", "XZ-LZMA2", ".xz","application/x-lzma"),
    COMPRESSION_FORMAT_LZMA("lzma", "LZMA", ".lzma","application/x-lzma"),
    COMPRESSION_FORMAT_SNAPPY("snappy", "Snappy", ".snappy","application/x-snappy"),
    COMPRESSION_FORMAT_SNAPPY_HADOOP("snappy-hadoop", "Snappy-Hadoop", ".snappy","application/x-snappy-hadoop"),
    COMPRESSION_FORMAT_SNAPPY_FRAMED("snappy framed", "Snappy-Framed", ".sz","application/x-snappy-framed"),
    COMPRESSION_FORMAT_LZ4_FRAMED("lz4-framed", "LZ4", ".lz4","application/x-lz4-framed"),
    COMPRESSION_FORMAT_ZSTD("zstd", "ZSTD", ".zst","application/zstd"),
    COMPRESSION_FORMAT_BROTLI("brotli", "Brotli", ".br","application/x-brotli");

    private final String description;
    private final String value;
    private final String fileExtension;
    private final String[] mimeTypes;

    public static CompressionInfo fromAllowableValue(String allowableValue) {
        for (CompressionInfo compressionInfo : CompressionInfo.values()) {
            if (compressionInfo.getValue().equalsIgnoreCase(allowableValue)) {
                return compressionInfo;
            }
        }
        return null;
    }

    CompressionInfo(final String value, final String description, final String fileExtension, final String... mimeTypes) {
        this.value = value;
        this.description = description;
        this.fileExtension = fileExtension;
        this.mimeTypes = mimeTypes;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return value;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public String getFileExtension() {
        return fileExtension;
    }

    public String[] getMimeTypes() {
        return mimeTypes;
    }

    public AllowableValue asAllowableValue() {
        return new AllowableValue(value, value, description);
    }
}
