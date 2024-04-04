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
package org.apache.nifi.processors.model;

import java.util.HashMap;
import java.util.Set;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class DatabaseField {
    private String tsName;
    private TSDataType dataType;
    private TSEncoding encoding;
    private CompressionType compressionType;

    private static final HashMap<String, TSDataType> typeMap = new HashMap<>();
    private static final HashMap<String, TSEncoding> encodingMap = new HashMap<>();

    private static final HashMap<String, CompressionType> compressionMap = new HashMap<>();

    static {
        typeMap.put("INT32", TSDataType.INT32);
        typeMap.put("INT64", TSDataType.INT64);
        typeMap.put("FLOAT", TSDataType.FLOAT);
        typeMap.put("DOUBLE", TSDataType.DOUBLE);
        typeMap.put("BOOLEAN", TSDataType.BOOLEAN);
        typeMap.put("TEXT", TSDataType.TEXT);

        encodingMap.put("PLAIN", TSEncoding.PLAIN);
        encodingMap.put("DICTIONARY", TSEncoding.DICTIONARY);
        encodingMap.put("RLE", TSEncoding.RLE);
        encodingMap.put("DIFF", TSEncoding.DIFF);
        encodingMap.put("TS_2DIFF", TSEncoding.TS_2DIFF);
        encodingMap.put("BITMAP", TSEncoding.BITMAP);
        encodingMap.put("GORILLA_V1", TSEncoding.GORILLA_V1);
        encodingMap.put("REGULAR", TSEncoding.REGULAR);
        encodingMap.put("GORILLA", TSEncoding.GORILLA);

        compressionMap.put("UNCOMPRESSED", CompressionType.UNCOMPRESSED);
        compressionMap.put("SNAPPY", CompressionType.SNAPPY);
        compressionMap.put("GZIP", CompressionType.GZIP);
        compressionMap.put("LZ4", CompressionType.LZ4);
    }

    public DatabaseField() {

    }

    public DatabaseField(String tsName, TSDataType dataType) {
        this.tsName = tsName;
        this.dataType = dataType;
    }

    public String getTsName() {
        return tsName;
    }

    public void setTsName(String tsName) {
        this.tsName = tsName;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TSEncoding getEncoding() {
        return encoding;
    }

    public void setEncoding(TSEncoding encoding) {
        this.encoding = encoding;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public static Set<String> getSupportedDataType() {
        return typeMap.keySet();
    }

    public static Set<String> getSupportedEncoding() {
        return encodingMap.keySet();
    }

    public static Set<String> getSupportedCompressionType() {
        return compressionMap.keySet();
    }
}
