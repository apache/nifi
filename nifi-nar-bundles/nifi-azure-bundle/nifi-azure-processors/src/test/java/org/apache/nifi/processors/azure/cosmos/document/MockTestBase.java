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

import java.util.Random;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.services.azure.cosmos.document.AzureCosmosDocumentConnectionControllerService;
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
        if(testRunner != null) {
            testRunner.setProperty(GetCosmosDocument.DATABASE_NAME, MOCK_DB_NAME);
            testRunner.setProperty(GetCosmosDocument.CONTAINER_ID,MOCK_CONTAINER_ID);
            testRunner.setProperty(GetCosmosDocument.PARTITION_KEY,MOCK_PARTITION_FIELD_NAME);
            if(withConnectionService) {
                // setup connnection controller service
                AzureCosmosDocumentConnectionControllerService service = new AzureCosmosDocumentConnectionControllerService();
                testRunner.addControllerService("connService", service);
                testRunner.setProperty(service, AzureCosmosDocumentConnectionControllerService.URI, MOCK_URI);
                testRunner.setProperty(service, AzureCosmosDocumentConnectionControllerService.DB_ACCESS_KEY, MOCK_DB_ACCESS_KEY);

                // now, after enabling and setting the service, it should be valid
                testRunner.enableControllerService(service);
                testRunner.setProperty(AbstractCosmosDocumentProcessor.CONNECTION_SERVICE, "connService");
            }
        }
    }

    private static Random random = new Random();
    public static int getRandomInt(int min, int max){
        return random.nextInt((max-min)+1) + min;
    }


}