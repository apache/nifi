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
package org.apache.nifi.processors.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.nifi.components.AllowableValue;

/**
 * Compression Type Enum for Hadoop related processors.
 */
public enum CompressionType {

    NONE("No compression"),
    DEFAULT("Default ZLIB compression"),
    BZIP("BZIP compression"),
    GZIP("GZIP compression"),
    LZ4("LZ4 compression"),
    LZO("LZO compression - it assumes LD_LIBRARY_PATH has been set and jar is available"),
    SNAPPY("Snappy compression"),
    AUTOMATIC("Will attempt to automatically detect the compression codec.");

    private final String description;

    CompressionType(String description) {
        this.description = description;
    }

    private String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return switch (this) {
            case NONE -> "NONE";
            case DEFAULT -> DefaultCodec.class.getName();
            case BZIP -> BZip2Codec.class.getName();
            case GZIP -> GzipCodec.class.getName();
            case LZ4 -> Lz4Codec.class.getName();
            case LZO -> "com.hadoop.compression.lzo.LzoCodec";
            case SNAPPY -> SnappyCodec.class.getName();
            case AUTOMATIC -> "Automatically Detected";
        };
    }

    public static AllowableValue[] allowableValues() {
        List<AllowableValue> values = new ArrayList<>();
        for (CompressionType type : CompressionType.values()) {
            values.add(new AllowableValue(type.name(), type.name(), type.getDescription()));
        }
        return values.toArray(new AllowableValue[values.size()]);
    }

}
