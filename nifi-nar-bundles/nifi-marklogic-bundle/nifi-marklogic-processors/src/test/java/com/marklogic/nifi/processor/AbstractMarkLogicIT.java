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

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.DeleteListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.nifi.controller.DatabaseClientService;
import com.marklogic.nifi.controller.DefaultDatabaseClientService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertTrue;

public class AbstractMarkLogicIT {
    protected String hostName = "localhost";
    protected String port = "8000";
    protected String database = "Documents";
    protected String username = "admin";
    protected String password = "admin";
    protected String authentication= "DIGEST";
    protected DatabaseClientService service;
    protected String batchSize = "3";
    protected String threadCount = "3";
    protected String databaseClientServiceIdentifier = "databaseClientService";
    protected int numDocs = 30;
    protected DatabaseClient client;
    protected DataMovementManager dataMovementManager;

    class IngestDoc {
        private Map<String, String> attributes;
        private String content;

        IngestDoc(String content) {
            this.attributes = new HashMap<>();
            this.content = content;
        }
        IngestDoc(String fileName, String content) {
            this(content);
            addAttribute("filename", fileName);
        }

        public String getFileName() {
            return attributes.getOrDefault("filename","uuid");
        }

        public String getContent() {
            return this.content;
        }

        public void addAttribute(String key, String value) {
            attributes.put(key, value);
        }

        public Map<String,String> getAttributes() {
            return this.attributes;
        }
    }
    protected List<IngestDoc> documents;

    protected void setup() {
        documents = new ArrayList<>(numDocs);
        client = DatabaseClientFactory.newClient(hostName, Integer.parseInt(port), new DatabaseClientFactory.DigestAuthContext(username, password));
        dataMovementManager = client.newDataMovementManager();
        for(int i = 0; i < numDocs; i++) {
            String fileName = "/PutMarkLogicTest/";
            String content = "";
            if(i % 5 == 0) {
                fileName += i + ".xml";
                content = "<sample>xmlcontent</sample>";
            } else if ( i % 3 == 0) {
                fileName += i + ".json";
                content = "{\"sample\":\"jsoncontent\"}";
            } else if (i % 7 == 0) {
                fileName += i + ".txt";
                content = "A sample text document";
            } else {
                fileName += i + ".png";
                content = "7sndalsdamasa";
            }
            documents.add(new IngestDoc(fileName, content));
        }
    }

    protected void addDatabaseClientService(TestRunner runner) throws InitializationException {
        service = new DefaultDatabaseClientService();
        runner.addControllerService(databaseClientServiceIdentifier, service);
        runner.setProperty(service, DefaultDatabaseClientService.HOST, hostName);
        runner.setProperty(service, DefaultDatabaseClientService.PORT, port);runner.setProperty(service, DefaultDatabaseClientService.DATABASE, database);
        runner.setProperty(service, DefaultDatabaseClientService.USERNAME, username);
        runner.setProperty(service, DefaultDatabaseClientService.PASSWORD, password);
        runner.setProperty(service, DefaultDatabaseClientService.SECURITY_CONTEXT_TYPE, authentication);
        runner.enableControllerService(service);
    }

    protected void teardown() {}

    protected TestRunner getNewTestRunner(Class processor) throws InitializationException {
        TestRunner runner = TestRunners.newTestRunner(processor);
        addDatabaseClientService(runner);
        runner.setProperty(AbstractMarkLogicProcessor.BATCH_SIZE, batchSize);
        runner.setProperty(AbstractMarkLogicProcessor.THREAD_COUNT, threadCount);
        assertTrue(runner.isControllerServiceEnabled(service));
        runner.assertValid(service);
        runner.setProperty(AbstractMarkLogicProcessor.DATABASE_CLIENT_SERVICE, databaseClientServiceIdentifier);
        return runner;
    }

    protected int getNumDocumentsInCollection(String collection) {
        StructuredQueryDefinition collectionQuery = new StructuredQueryBuilder().collection(collection);
        AtomicInteger actualNumberOfDocs = new AtomicInteger(0);
        QueryBatcher queryBatcher = dataMovementManager.newQueryBatcher(collectionQuery)
                .onUrisReady(queryBatch -> actualNumberOfDocs.addAndGet(queryBatch.getItems().length));
        dataMovementManager.startJob(queryBatcher);
        queryBatcher.awaitCompletion();
        dataMovementManager.stopJob(queryBatcher);
        return actualNumberOfDocs.get();
    }

    protected void deleteDocumentsInCollection(String collection) {
        StructuredQueryDefinition collectionQuery = new StructuredQueryBuilder().collection(collection);
        QueryBatcher queryBatcher = dataMovementManager.newQueryBatcher(collectionQuery)
          .withConsistentSnapshot()
          .onUrisReady(new DeleteListener());
        dataMovementManager.startJob(queryBatcher);
        queryBatcher.awaitCompletion();
        dataMovementManager.stopJob(queryBatcher);
    }
}
