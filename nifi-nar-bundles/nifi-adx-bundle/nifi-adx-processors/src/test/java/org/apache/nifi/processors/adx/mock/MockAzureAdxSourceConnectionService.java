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

import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.nifi.adx.service.StandardKustoQueryService;

import java.io.InputStream;

/**
 * these are mock implementation classes of adx source connection service required for junit classes to work properly
 */
public class MockAzureAdxSourceConnectionService extends StandardKustoQueryService {

    @Override
    public StreamingClient getKustoQueryClient(){
        return new StreamingClient() {

            @Override
            public void close() {
            }

            @Override
            public KustoOperationResult executeStreamingIngest(String s,
                                                               String s1,
                                                               InputStream inputStream,
                                                               ClientRequestProperties clientRequestProperties,
                                                               String s2,
                                                               String s3,
                                                               boolean b) throws DataServiceException, DataClientException {
                return null;
            }

            @Override
            public InputStream executeStreamingQuery(String s, String s1, ClientRequestProperties clientRequestProperties) throws DataServiceException, DataClientException {
                return null;
            }

            @Override
            public InputStream executeStreamingQuery(String s, String s1) throws DataClientException {
               return this.getClass().getResourceAsStream("/Test.json");
            }

            @Override
            public InputStream executeStreamingQuery(String s) throws DataServiceException, DataClientException {
                return null;
            }

            @Override
            public KustoOperationResult execute(String command) {
                return null;
            }
        };
    }
}
