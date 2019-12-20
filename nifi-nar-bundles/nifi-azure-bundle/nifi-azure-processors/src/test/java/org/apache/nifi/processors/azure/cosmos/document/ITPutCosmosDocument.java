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

import java.util.logging.Logger;
import org.apache.nifi.processor.Processor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ITPutCosmosDocument extends ITAbstractCosmosDocument {
    static Logger logger = Logger.getLogger(ITPutCosmosDocument.class.getName());

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return PutCosmosDocument.class;
    }

    @Before
    public void setUp() throws Exception {
        resetTestCosmosConnection();
        runner.setIncomingConnection(false);
        runner.setNonLoopConnection(false);
    }

    @After
    public void cleanupTestCase() {
        try{
            clearTestData();
            closeClient();
        }catch(Exception e) {

        }
    }

    @Test
    public void testOnTriggerWithoutDocumentId() {
        resetTestCosmosConnection();
        String document = "{\"hello\": 2, \"category\": \"cat-x\"}";
        runner.enqueue(document.getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(PutCosmosDocument.REL_SUCCESS, 1);
    }

    @Test
    public void testOnTriggerWithDocumentIntId() {
        resetTestCosmosConnection();
        String document = "{\"id\": 2, \"category\": \"cat-y\"}";
        runner.enqueue(document.getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(PutCosmosDocument.REL_SUCCESS, 1);
    }

    @Test
    public void testOnTriggerWithMalformattedDocument() {
        resetTestCosmosConnection();
        String document = "{\"id\": 2 \"category\": \"cat-y\"}";
        runner.enqueue(document.getBytes());
        runner.run();
        runner.assertTransferCount(PutCosmosDocument.REL_FAILURE, 1);
        runner.assertTransferCount(PutCosmosDocument.REL_SUCCESS, 0);

    }

    @Test
    public void testOnTriggerWithNoPartitionKeyValue() {
        // PutCosmosDocument will reject the data without partition key value
        // since this processor supports insert/update
        // On the contrary, PutCosmosDocumentRecord doesnot support update
        // thus, it will accept data without partition key value.

        resetTestCosmosConnection();
        logger.info("starting testOnTriggerWithNoPartitionKeyValue");
        String document = "{\"id\": 4}";
        runner.enqueue(document.getBytes());
        runner.run();
        logger.info("ending testOnTriggerWithNoPartitionKeyValue");
        runner.assertTransferCount(PutCosmosDocument.REL_FAILURE, 1);
        runner.assertTransferCount(PutCosmosDocument.REL_SUCCESS, 0);

    }
}