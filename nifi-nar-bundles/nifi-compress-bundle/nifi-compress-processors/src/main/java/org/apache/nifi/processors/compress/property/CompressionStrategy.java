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
package org.apache.nifi.processors.compress.property;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;

import java.util.Arrays;
import java.util.Optional;

public enum CompressionStrategy implements DescribedValue {

    NONE("no compression", "No Compression", ""),
    MIME_TYPE_ATTRIBUTE("use mime.type attribute", "Use the [mime.type] attribute from the input FlowFile to determine the format", ""),
    GZIP("gzip", "GZIP", ".gz","application/gzip", "application/x-gzip"),
    DEFLATE("deflate", "Deflate", ".zlib","application/deflate", "application/x-deflate"),
    BZIP2("bzip2", "BZIP2", ".bz2","application/x-bzip2", "application/bzip2"),
    XZ_LZMA2("xz-lzma2", "XZ-LZMA2", ".xz","application/x-lzma"),
    LZMA("lzma", "LZMA", ".lzma","application/x-lzma"),
    SNAPPY("snappy", "Snappy", ".snappy","application/x-snappy"),
    SNAPPY_HADOOP("snappy-hadoop", "Snappy-Hadoop", ".snappy","application/x-snappy-hadoop"),
    SNAPPY_FRAMED("snappy-framed", "Snappy-Framed", ".sz","application/x-snappy-framed"),
    LZ4_FRAMED("lz4-framed", "LZ4", ".lz4","application/x-lz4-framed"),
    ZSTD("zstd", "ZSTD", ".zst","application/zstd"),
    BROTLI("brotli", "Brotli", ".br","application/x-brotli");

    private final String description;
    private final String value;
    private final String fileExtension;
    private final String[] mimeTypes;

    public static Optional<CompressionStrategy> findValue(final String value) {
        return Arrays.stream(CompressionStrategy.values())
                .filter((compressionStrategy -> compressionStrategy.getValue().equalsIgnoreCase(value)))
                .findFirst();
    }

    CompressionStrategy(final String value, final String description, final String fileExtension, final String... mimeTypes) {
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
