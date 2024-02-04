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
package org.apache.nifi.services.azure.data.explorer;

import java.io.InputStream;
import java.time.Duration;

public class KustoIngestionRequest {

    private final boolean streamingEnabled;
    private final boolean pollOnIngestionStatus;
    private final InputStream inputStream;
    private final String dataFormat;
    private final String mappingName;
    private final String databaseName;
    private final String tableName;
    private final boolean ignoreFirstRecord;
    private final Duration ingestionStatusPollingTimeout;
    private final Duration ingestionStatusPollingInterval;

    public KustoIngestionRequest(final boolean isStreamingEnabled,
                                 final boolean pollOnIngestionStatus,
                                 final InputStream inputStream,
                                 final String databaseName,
                                 final String tableName,
                                 final String dataFormat,
                                 final String mappingName,
                                 final boolean ignoreFirstRecord,
                                 final Duration ingestionStatusPollingTimeout,
                                 final Duration ingestionStatusPollingInterval) {
        this.streamingEnabled = isStreamingEnabled;
        this.inputStream = inputStream;
        this.pollOnIngestionStatus = pollOnIngestionStatus;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.dataFormat = dataFormat;
        this.mappingName = mappingName;
        this.ignoreFirstRecord = ignoreFirstRecord;
        this.ingestionStatusPollingInterval = ingestionStatusPollingInterval;
        this.ingestionStatusPollingTimeout = ingestionStatusPollingTimeout;
    }

    public boolean isStreamingEnabled() {
        return streamingEnabled;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public boolean pollOnIngestionStatus() {
        return pollOnIngestionStatus;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isIgnoreFirstRecord() {
        return ignoreFirstRecord;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public String getMappingName() {
        return mappingName;
    }

    public Duration getIngestionStatusPollingTimeout() {
        return ingestionStatusPollingTimeout;
    }

    public Duration getIngestionStatusPollingInterval() {
        return ingestionStatusPollingInterval;
    }
}
