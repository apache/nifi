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

import com.microsoft.azure.kusto.ingest.IngestionProperties;

import java.io.InputStream;

public class KustoIngestionRequest {

    private boolean isStreamingEnabled;

    private boolean pollOnIngestionStatus;
    private InputStream inputStream;
    private IngestionProperties ingestionProperties;

    public KustoIngestionRequest(boolean isStreamingEnabled, boolean pollOnIngestionStatus , InputStream inputStream, IngestionProperties ingestionProperties) {
        this.isStreamingEnabled = isStreamingEnabled;
        this.inputStream = inputStream;
        this.ingestionProperties = ingestionProperties;
        this.pollOnIngestionStatus = pollOnIngestionStatus;
    }

    public boolean isStreamingEnabled() {
        return isStreamingEnabled;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public IngestionProperties getIngestionProperties() {
        return ingestionProperties;
    }

    public boolean pollOnIngestionStatus() {
        return pollOnIngestionStatus;
    }

    public void setPollOnIngestionStatus(boolean pollOnIngestionStatus) {
        this.pollOnIngestionStatus = pollOnIngestionStatus;
    }
}
