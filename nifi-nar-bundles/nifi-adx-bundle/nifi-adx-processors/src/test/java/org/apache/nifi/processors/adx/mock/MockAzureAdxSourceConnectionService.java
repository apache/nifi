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

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.adx.AzureAdxSourceConnectionService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * these are mock implementation classes of adx source connection service required for junit classes to work properly
 */
public class MockAzureAdxSourceConnectionService extends AzureAdxSourceConnectionService {

    @Override
    public Client getKustoQueryClient(){
        return new Client() {

            @Override
            public void close() throws IOException {
            }
            @Override
            public KustoOperationResult execute(String command) {
                return null;
            }

            @Override
            public KustoOperationResult execute(String database, String command) throws DataClientException {
                try {
                    String response = IOUtils.toString(Objects.requireNonNull(this.getClass().getResourceAsStream("/Test.json")), StandardCharsets.UTF_8);
                    return new KustoOperationResult(response, "v1");
                } catch (KustoServiceQueryError | IOException e) {
                    throw new DataClientException("exception","exception",e);
                }
            }

            @Override
            public KustoOperationResult execute(String database, String command, ClientRequestProperties properties) {
                return null;
            }

            @Override
            public String executeToJsonResult(String database) {
                return null;
            }

            @Override
            public String executeToJsonResult(String database, String command) {
                return null;
            }

            @Override
            public String executeToJsonResult(String database, String command, ClientRequestProperties properties) {
                return null;
            }
        };
    }
}
