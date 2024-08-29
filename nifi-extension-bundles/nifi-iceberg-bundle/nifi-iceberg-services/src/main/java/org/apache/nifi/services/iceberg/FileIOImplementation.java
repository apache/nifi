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
package org.apache.nifi.services.iceberg;

import org.apache.nifi.components.DescribedValue;

import java.util.HashMap;
import java.util.Map;

public enum FileIOImplementation implements DescribedValue {
    HADOOP( "org.apache.iceberg.hadoop.HadoopFileIO", "Hadoop File IO"),
    RESOLVING("org.apache.iceberg.io.ResolvingFileIO", "Resolving File IO"),
    S3( "org.apache.iceberg.aws.s3.S3FileIO", "S3 File IO"),
    GCS( "org.apache.iceberg.gcp.gcs.GCSFileIO", "GCS File IO"),
    ADLS( "org.apache.iceberg.azure.adlsv2.ADLSFileIO", "ADLS File IO");

    private static final Map<String, FileIOImplementation> ENUM_MAP = new HashMap<>();

    static {
        for (FileIOImplementation strategy : FileIOImplementation.values()) {
            ENUM_MAP.put(strategy.getValue(), strategy);
        }
    }

    private final String value;
    private final String displayName;
    private final String description;

    FileIOImplementation(String value, String displayName) {
        this(value, displayName, null);
    }

    FileIOImplementation(String value, String displayName, String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
