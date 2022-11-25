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
package org.apache.nifi.processors.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import org.apache.nifi.adx.AzureAdxSinkSinkConnectionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MockAzureAdxSinkSinkConnectionService extends AzureAdxSinkSinkConnectionService {

    @Override
    public Client getKustoExecutionClient(){
        return new Client() {
            @Override
            public KustoOperationResult execute(String command) throws DataServiceException, DataClientException {
                return null;
            }

            @Override
            public KustoOperationResult execute(String database, String command) throws DataServiceException, DataClientException {
                return null;
            }

            @Override
            public KustoOperationResult execute(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException {
                return null;
            }

            @Override
            public String executeToJsonResult(String database) throws DataServiceException, DataClientException {
                return null;
            }

            @Override
            public String executeToJsonResult(String database, String command) throws DataServiceException, DataClientException {
                return null;
            }

            @Override
            public String executeToJsonResult(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException {
                return null;
            }
        };
    }

    @Override
    public IngestClient getAdxClient(boolean isStreamingEnabled) {
        return new IngestClient() {
            @Override
            public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
                return null;
            }

            @Override
            public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
                return null;
            }

            @Override
            public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
                return new IngestionResult() {
                    @Override
                    public List<IngestionStatus> getIngestionStatusCollection() {
                        List<IngestionStatus> ingestionStatusList = new ArrayList<>();
                        IngestionStatus ingestionStatus = new IngestionStatus();
                        ingestionStatus.status = OperationStatus.Succeeded;
                        ingestionStatusList.add(ingestionStatus);
                        return ingestionStatusList;
                    }

                    @Override
                    public int getIngestionStatusesLength() {
                        return 0;
                    }
                };
            }

            @Override
            public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
                return new IngestionResult() {
                    @Override
                    public List<IngestionStatus> getIngestionStatusCollection() {
                        List<IngestionStatus> ingestionStatusList = new ArrayList<>();
                        IngestionStatus ingestionStatus = new IngestionStatus();
                        ingestionStatus.status = OperationStatus.Succeeded;
                        ingestionStatusList.add(ingestionStatus);
                        return ingestionStatusList;
                    }

                    @Override
                    public int getIngestionStatusesLength() {
                        return 0;
                    }
                };
            }

            @Override
            public void close() throws IOException {

            }
        };
    }
}
