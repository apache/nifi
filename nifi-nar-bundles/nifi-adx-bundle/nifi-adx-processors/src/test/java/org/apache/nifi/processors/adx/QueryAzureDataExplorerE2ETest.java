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

import org.apache.nifi.adx.AzureAdxSourceConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.HashMap;

/**
 * These are end to end tests for ADX Source Processor, which are disabled/skipped by default
 * during maven test phase. These parameters need to be provided during maven build
 * -DexecuteE2ETests=(set to true)
 * -DappId=(appId)
 * -DappKey=(appKey)
 * -DappTenant=(appTenant)
 * -DclusterUrl=(clusterUrl)
 * -DdatabaseName=(databaseName)
 * -DadxQuery=(query-to-be-executed-in-source-ADX)
 * -DadxQueryWhichExceedsLimit=(query-whose-execution-will-exceed-the-kusto-query-limits)
 **/

@EnabledIfSystemProperty(named = "executeE2ETests", matches = "true")
class QueryAzureDataExplorerE2ETest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() throws InitializationException {

        QueryAzureDataExplorer queryAzureDataExplorer = new QueryAzureDataExplorer();
        testRunner = TestRunners.newTestRunner(queryAzureDataExplorer);
        testRunner.setValidateExpressionUsage(false);
        AzureAdxSourceConnectionService azureAdxSourceConnectionService = new AzureAdxSourceConnectionService();
        testRunner.addControllerService("adx-connection-service", azureAdxSourceConnectionService, new HashMap<>());
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_ID,System.getProperty("appId"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_KEY,System.getProperty("appKey"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.APP_TENANT,System.getProperty("appTenant"));
        testRunner.setProperty(azureAdxSourceConnectionService, AzureAdxSourceConnectionService.CLUSTER_URL, System.getProperty("clusterUrl"));
        testRunner.enableControllerService(azureAdxSourceConnectionService);
        testRunner.assertValid(azureAdxSourceConnectionService);
    }

    /**
     * tests the successful scenario when all the parameters are passed
     * queries the ADX database against the input query and passes the result to flow file of RL_SUCCEEDED
     * -DexecuteE2ETests=(set to true)
     * -DappId=(appId)
     * -DappKey=(appKey)
     * -DappTenant=(appTenant)
     * -DclusterUrl=(clusterUrl)
     * -DdatabaseName=(databaseName)
     * -DadxQuery=<(query-to-be-executed-in-source-ADX)
     */
    @Test
    void testAzureAdxSourceProcessorSuccessE2E() {
        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner.setProperty(QueryAzureDataExplorer.ADX_QUERY,System.getProperty("adxQuery"));
        testRunner.setProperty(QueryAzureDataExplorer.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(QueryAzureDataExplorer.ADX_SOURCE_SERVICE,"adx-connection-service");
        testRunner.setIncomingConnection(false);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(QueryAzureDataExplorer.SUCCESS);
    }

    /**
     * tests the failure scenario when the kusto query limit is execeeded
     * queries the ADX database against the input query, query result records/size too large, results in RL_FAILURE
     * -DexecuteE2ETests=(set to true)
     * -DappId=(appId)
     * -DappKey=(appKey)
     * -DappTenant=(appTenant)
     * -DclusterUrl=(clusterUrl)
     * -DdatabaseName=(databaseName)
     * -adxQueryWhichExceedsLimit=(query-to-be-executed-in-source-ADX-which-exceeds-the-kusto-limits-500000records/64MB)
     */
    @Test
    void testAzureAdxSourceProcessorFailureQueryLimitExceededE2E() {
        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getProperty("executeE2ETests")));

        testRunner.setProperty(QueryAzureDataExplorer.ADX_QUERY,System.getProperty("adxQueryWhichExceedsLimit"));
        testRunner.setProperty(QueryAzureDataExplorer.DB_NAME,System.getProperty("databaseName"));
        testRunner.setProperty(QueryAzureDataExplorer.ADX_SOURCE_SERVICE,"adx-connection-service");
        testRunner.setIncomingConnection(false);
        testRunner.run(1);
        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred(QueryAzureDataExplorer.FAILED);
    }

}
