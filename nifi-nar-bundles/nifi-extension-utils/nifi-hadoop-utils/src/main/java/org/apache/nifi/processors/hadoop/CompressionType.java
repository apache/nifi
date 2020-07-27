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

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;

/**
 * Compression Type Enum for Hadoop related processors.
 */
public enum CompressionType {
    NONE,
    DEFAULT,
    BZIP,
    GZIP,
    LZ4,
    SNAPPY,
    AUTOMATIC;

    @Override
    public String toString() {
        switch (this) {
            case NONE: return "NONE";
            case DEFAULT: return DefaultCodec.class.getName();
            case BZIP: return BZip2Codec.class.getName();
            case GZIP: return GzipCodec.class.getName();
            case LZ4: return Lz4Codec.class.getName();
            case SNAPPY: return SnappyCodec.class.getName();
            case AUTOMATIC: return "Automatically Detected";
        }
        return null;
    }

}
