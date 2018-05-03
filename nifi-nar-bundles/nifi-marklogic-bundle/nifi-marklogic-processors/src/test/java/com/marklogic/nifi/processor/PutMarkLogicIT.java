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
package com.marklogic.nifi.processor;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PutMarkLogicIT extends AbstractMarkLogicIT{
    private String collection;

     @Before
    public void setup() {
        super.setup();
        collection = "PutMarkLogicTest";
        deleteDocumentsInCollection(collection);
    }

    @After
    public void teardown() {
        super.teardown();
        deleteDocumentsInCollection(collection);
    }

    public TestRunner getNewTestRunner(Class processor) throws InitializationException {
        TestRunner runner = super.getNewTestRunner(processor);
        runner.setProperty(PutMarkLogic.COLLECTIONS, collection);
        runner.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "filename");
        return runner;
    }

    @Test
    public void simpleIngest() throws InitializationException, InterruptedException {
        TestRunner runner = getNewTestRunner(PutMarkLogic.class);
        for(IngestDoc document : documents) {
            runner.enqueue(document.getContent(), document.getAttributes());
        }
        Map<String, String> attributesMap = new HashMap<>();
        attributesMap.put("filename", "invalidjson.json");
        runner.run(numDocs);
        runner.enqueue("{sdsfsd}", attributesMap);
        runner.enqueue("{sdsfsd}", attributesMap);
        runner.run(2);
        runner.assertQueueEmpty();
        assertEquals(2,runner.getFlowFilesForRelationship(PutMarkLogic.FAILURE).size());
        assertEquals(numDocs,runner.getFlowFilesForRelationship(PutMarkLogic.SUCCESS).size());
        assertEquals(numDocs, getNumDocumentsInCollection(collection));
    }
}
