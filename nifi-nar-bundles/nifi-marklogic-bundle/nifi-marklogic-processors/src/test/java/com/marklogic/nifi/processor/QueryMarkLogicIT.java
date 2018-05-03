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

import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryMarkLogicIT extends AbstractMarkLogicIT {
    private String collection;

    @Before
    public void setup() {
        super.setup();
        collection = "QueryMarkLogicTest";
        // Load documents to Query
        loadDocumentsIntoCollection(collection, documents);
    }

    private void loadDocumentsIntoCollection(String collection, List<IngestDoc> documents) {
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher()
            .withBatchSize(3)
            .withThreadCount(3);
        dataMovementManager.startJob(writeBatcher);
        for(IngestDoc document : documents) {
            DocumentMetadataHandle handle = new DocumentMetadataHandle();
            handle.withCollections(collection);
            writeBatcher.add(document.getFileName(), handle, new StringHandle(document.getContent()));
        }
        writeBatcher.flushAndWait();
        dataMovementManager.stopJob(writeBatcher);
    }

    @After
    public void teardown() {
        super.teardown();
        deleteDocumentsInCollection(collection);
    }

    protected TestRunner getNewTestRunner(Class processor) throws InitializationException {
        TestRunner runner = super.getNewTestRunner(processor);
        runner.setProperty(QueryMarkLogic.COLLECTIONS, collection);
        runner.setProperty(QueryMarkLogic.CONSISTENT_SNAPSHOT, "true");
        return runner;
    }

    @Test
    public void testSimpleCollectionQuery() throws InitializationException {
        loadDocumentsIntoCollection(collection, documents);
        TestRunner runner = getNewTestRunner(QueryMarkLogic.class);
        runner.run();
        runner.assertTransferCount(QueryMarkLogic.SUCCESS, numDocs);
        runner.assertAllFlowFilesContainAttribute(QueryMarkLogic.SUCCESS,"uri");
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QueryMarkLogic.SUCCESS);
        byte[] actualByteArray = null;
        for(MockFlowFile flowFile : flowFiles) {
            if(flowFile.getAttribute("uri").endsWith("3.json")) {
                actualByteArray = runner.getContentAsByteArray(flowFile);
                break;
            }
        }
        byte[] expectedByteArray = documents.get(3).getContent().getBytes();
        assertEquals(expectedByteArray.length, actualByteArray.length);
        assertTrue(Arrays.equals(expectedByteArray, actualByteArray));
    }
}
