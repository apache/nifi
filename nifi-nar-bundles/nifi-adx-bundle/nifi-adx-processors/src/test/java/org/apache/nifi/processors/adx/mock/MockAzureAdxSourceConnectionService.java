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
package org.apache.nifi.processors.adx.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import org.apache.nifi.adx.AzureAdxSourceConnectionService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MockAzureAdxSourceConnectionService extends AzureAdxSourceConnectionService {

    @Override
    public Client getKustoExecutionClient(){
        return new Client() {
            @Override
            public void close() throws IOException {

            }

            @Override
            public KustoOperationResult execute(String command) throws DataServiceException, DataClientException {
                return null;
            }

            @Override
            public KustoOperationResult execute(String database, String command) throws DataServiceException, DataClientException {
                ObjectMapper objectMapper = Utils.getObjectMapper();
                List<List<String>> valuesList = new ArrayList<>();
                valuesList.add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue"))));
                valuesList.add(new ArrayList<>((Arrays.asList("SecuredReadyForAggregationQueue"))));
                valuesList.add(new ArrayList<>((Arrays.asList("FailedIngestionsQueue"))));
                valuesList.add(new ArrayList<>((Arrays.asList("SuccessfulIngestionsQueue"))));
                valuesList.add(new ArrayList<>((Arrays.asList("TempStorage"))));
                valuesList.add(new ArrayList<>((Arrays.asList("TempStorage"))));
                valuesList.add(new ArrayList<>((Arrays.asList("IngestionsStatusTable"))));
                String listAsJson = null;
                try {
                    listAsJson = objectMapper.writeValueAsString(valuesList);
                String response = "{\"Tables\":[{\"TableName\":\"Table_0\",\"Columns\":[{\"ColumnName\":\"ResourceTypeName\"," +
                        "\"DataType\":\"String\",\"ColumnType\":\"string\"},{\"ColumnName\":\"StorageRoot\",\"DataType\":" +
                        "\"String\",\"ColumnType\":\"string\"}],\"Rows\":"
                        + listAsJson + "}]}";
                    return new KustoOperationResult(response, "v1");
                } catch (KustoServiceQueryError | JsonProcessingException e) {
                    throw new DataClientException("exception","exception",e);
                }
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
}
