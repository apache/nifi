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
package org.apache.nifi.marklogic.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.ExportListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.StructuredQueryBuilder;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PutMarkLogicIT extends AbstractMarkLogicIT{

     @Before
    public void setup() {
        super.setup();
    }

    @After
    public void teardown() {
        super.teardown();
    }

    public TestRunner getNewTestRunner(Class processor) throws InitializationException {
        TestRunner runner = super.getNewTestRunner(processor);
        runner.setProperty(PutMarkLogic.URI_ATTRIBUTE_NAME, "filename");
        return runner;
    }

    @Test
    public void simpleIngest() throws InitializationException {
        String collection = "PutMarkLogicTest";
        String absolutePath = "/dummyPath/PutMarkLogicTest";
        TestRunner runner = getNewTestRunner(PutMarkLogic.class);
        runner.setProperty(PutMarkLogic.COLLECTIONS, collection+",${absolutePath}");
        for(IngestDoc document : documents) {
            document.getAttributes().put("absolutePath", absolutePath);
            runner.enqueue(document.getContent(), document.getAttributes());
        }
        runner.run(numDocs);
        Map<String, String> attributesMap = new HashMap<>();
        attributesMap.put("filename", "invalidjson.json");
        runner.enqueue("{sdsfsd}", attributesMap);
        runner.enqueue("{sdsfsd}", attributesMap);
        runner.run(2);
        runner.assertQueueEmpty();
        assertEquals(2,runner.getFlowFilesForRelationship(PutMarkLogic.FAILURE).size());
        assertEquals(numDocs,runner.getFlowFilesForRelationship(PutMarkLogic.SUCCESS).size());
        assertEquals(numDocs, getNumDocumentsInCollection(collection));
        assertEquals(numDocs, getNumDocumentsInCollection(absolutePath));
        deleteDocumentsInCollection(collection);
    }

    @Test
    public void transformParameters() throws InitializationException, IOException {
        String collection = "transform";
        String transform = "servertransform";
        TestRunner runner = getNewTestRunner(PutMarkLogic.class);
        runner.setValidateExpressionUsage(false);
        runner.setProperty(PutMarkLogic.COLLECTIONS, collection);
        runner.setProperty(PutMarkLogic.TRANSFORM, transform);
        runner.setProperty("trans:newValue", "new");
        StringHandle stringHandle = new StringHandle(
            "function transform_function(context, params, content) {\n" +
            "  var document = content.toObject();\n" +
            "  document.testProperty = params.newValue;\n" +
            "  return document;\n" +
            "};\n" +
            "exports.transform = transform_function;");
        service.getDatabaseClient().newServerConfigManager().newTransformExtensionsManager().writeJavascriptTransform(
            transform, stringHandle);
        String content = "{ \"testProperty\": \"oldValue\" }";
        for(int i = 0; i < numDocs; i++) {
            Map<String, String> attributesMap = new HashMap<>();
            attributesMap.put("filename", "/transform/"+i+".json");
            runner.enqueue(content, attributesMap);
        }
        runner.run(numDocs);
        // Test if the transform went through
        DataMovementManager dataMovementManager = service.getDatabaseClient().newDataMovementManager();
        QueryBatcher queryBatcher = dataMovementManager.newQueryBatcher(new StructuredQueryBuilder().collection(collection))
            .onUrisReady(new ExportListener().onDocumentReady(documentRecord -> {
                assertEquals("new", documentRecord.getContentAs(JsonNode.class).get("testProperty").textValue());
            }));
        dataMovementManager.startJob(queryBatcher);
        queryBatcher.awaitCompletion();
        dataMovementManager.stopJob(queryBatcher);
        deleteDocumentsInCollection(collection);
    }
}
