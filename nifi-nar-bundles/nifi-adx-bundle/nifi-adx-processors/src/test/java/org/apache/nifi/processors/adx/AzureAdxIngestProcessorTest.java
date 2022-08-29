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


import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;



public class AzureAdxIngestProcessorTest {

    private TestRunner testRunner;

    private AzureAdxIngestProcessor azureAdxIngestProcessor;

    private MockProcessContext mockProcessContext;

    private MockProcessSession mockProcessSession;

    private MockComponentLog mockComponentLog;

    private static final String MOCK_DB_NAME= "mockDBName";

    private static final String MOCK_TABLE_NAME= "mockTableName";

    private static final String MOCK_MAPPING_NAME= "mockMappingName";


    private AzureAdxIngestProcessorTest adxIngestProcessorTest;

    @BeforeEach
    public void setup() {
        adxIngestProcessorTest = new AzureAdxIngestProcessorTest();
        //azureAdxIngestProcessor = new MockAzureAdxIngestProcessor();
        //mockProcessContext = new MockProcessContext(azureAdxIngestProcessor);
        //mockProcessContext.setProperty(AzureAdxIngestProcessor.DB_NAME,MOCK_DB_NAME);
        //mockProcessContext.setProperty(AzureAdxIngestProcessor.TABLE_NAME,MOCK_DB_NAME);

        //SharedSessionState sharedState = new SharedSessionState(azureAdxIngestProcessor, new AtomicLong(0));

        //mockProcessSession = new MockProcessSession(sharedState,azureAdxIngestProcessor);
        testRunner = TestRunners.newTestRunner(AzureAdxIngestProcessor.class,mockComponentLog);
    }

    @Test
    public void testAzureAdxIngestProcessor() {
        setup();
        //azureAdxIngestProcessor.onTrigger(mockProcessContext,mockProcessSession);



        //TestVerification.assertDatatFlowMetrics(collectedMetrics);
    }



}
