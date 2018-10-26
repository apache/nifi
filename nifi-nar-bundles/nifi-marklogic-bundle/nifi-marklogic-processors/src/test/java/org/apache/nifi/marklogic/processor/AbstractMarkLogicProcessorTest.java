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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.marklogic.controller.DefaultMarkLogicDatabaseClientService;
import org.apache.nifi.marklogic.controller.MarkLogicDatabaseClientService;
import org.apache.nifi.marklogic.controller.MarkLogicTestConfig;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.MockProcessorInitializationContext;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AbstractMarkLogicProcessorTest extends Assert {
    Processor processor;
    protected MockProcessContext processContext;
    protected MockProcessorInitializationContext initializationContext;
    protected SharedSessionState sharedSessionState;
    protected MockProcessSession processSession;
    protected TestRunner runner;
    protected MockProcessSessionFactory mockProcessSessionFactory;
    protected MarkLogicDatabaseClientService service;
    protected String databaseClientServiceIdentifier = "databaseClientService";
    protected String hostName = MarkLogicTestConfig.hostName;
    protected String port = MarkLogicTestConfig.port;
    protected String database = MarkLogicTestConfig.database;
    protected String username = MarkLogicTestConfig.username;
    protected String password = MarkLogicTestConfig.password;
    protected String loadBalancer= MarkLogicTestConfig.loadBalancer;
    protected String authentication= "CERTIFICATE";

    protected void initialize(Processor processor) throws InitializationException {
        this.processor = processor;
        processContext = new MockProcessContext(processor);
        initializationContext = new MockProcessorInitializationContext(processor, processContext);
        sharedSessionState = new SharedSessionState(processor, new AtomicLong());
        processSession = new MockProcessSession(sharedSessionState, processor);
        mockProcessSessionFactory = new MockProcessSessionFactory(sharedSessionState, processor);
        runner = TestRunners.newTestRunner(processor);
        service = new DefaultMarkLogicDatabaseClientService();
        runner.addControllerService(databaseClientServiceIdentifier, service);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.HOST, hostName);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PORT, port);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.DATABASE, database);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.USERNAME, username);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.PASSWORD, password);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.SECURITY_CONTEXT_TYPE, MarkLogicTestConfig.authentication);
    }
    protected MockFlowFile addFlowFile(String content) {
        MockFlowFile flowFile = processSession.createFlowFile(content.getBytes());
        sharedSessionState.getFlowFileQueue().offer(flowFile);
        return flowFile;
    }
}

class MockProcessSessionFactory implements ProcessSessionFactory {
    SharedSessionState sharedSessionState;
    Processor processor;
    MockProcessSessionFactory(SharedSessionState sharedSessionState, Processor processor) {
        this.sharedSessionState = sharedSessionState;
        this.processor = processor;
    }
    @Override
    public ProcessSession createSession() {
        return new MockProcessSession(sharedSessionState, processor);
    }
}