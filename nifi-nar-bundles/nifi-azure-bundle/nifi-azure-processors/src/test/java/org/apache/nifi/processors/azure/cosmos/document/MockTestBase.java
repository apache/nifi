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

package org.apache.nifi.processors.azure.cosmos.document;

import static org.mockito.Mockito.mock;

import java.util.Random;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.azure.cosmos.document.AzureCosmosDBClientService;
import org.apache.nifi.util.TestRunner;

public class MockTestBase {

    protected static final String MOCK_DB_NAME = "MOCK_DB_NAME";
    protected static final String MOCK_CONTAINER_ID = "MOCK_CONTAINER_ID";
    protected static final String MOCK_URI = "MOCK_URI";
    protected static final String MOCK_DB_ACCESS_KEY = "MOCK_DB_ACCESS_KEY";
    public static final String MOCK_QUERY = "select * from c";

    public static final String MOCK_PARTITION_FIELD_NAME = "category";
    protected TestRunner testRunner;

    protected void setBasicMockProperties(boolean withConnectionService) throws InitializationException {
        if (testRunner != null) {
            testRunner.setProperty(AbstractAzureCosmosDBProcessor.DATABASE_NAME, MOCK_DB_NAME);
            testRunner.setProperty(AbstractAzureCosmosDBProcessor.CONTAINER_ID,MOCK_CONTAINER_ID);
            testRunner.setProperty(AbstractAzureCosmosDBProcessor.PARTITION_KEY,MOCK_PARTITION_FIELD_NAME);
            if (withConnectionService) {
                // setup connnection controller service
                AzureCosmosDBClientService service = new MockConnectionService();
                testRunner.addControllerService("connService", service);
                testRunner.setProperty(service, AzureCosmosDBUtils.URI, MOCK_URI);
                testRunner.setProperty(service, AzureCosmosDBUtils.DB_ACCESS_KEY, MOCK_DB_ACCESS_KEY);

                // now, after enabling and setting the service, it should be valid
                testRunner.enableControllerService(service);
                testRunner.setProperty(AbstractAzureCosmosDBProcessor.CONNECTION_SERVICE, "connService");
            }
        }
    }

    private static Random random = new Random();
    public static int getRandomInt(int min, int max){
        return random.nextInt((max-min)+1) + min;
    }

    private class MockConnectionService extends AzureCosmosDBClientService {
        @Override
        protected void createCosmosClient(final String uri, final String accessKey, final ConsistencyLevel clevel){
            // mock cosmos client
            this.setCosmosClient(mock(CosmosClient.class));
        }
    }

}