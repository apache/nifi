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
package org.apache.nifi.rules.processors;

import com.google.common.collect.Lists;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.engine.RulesEngineService;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestRulesRecordProcessor {

    private TestRunner runner;
    private RulesRecordProcessor processor;
    private MockRecordParser parser;
    private JsonRecordSetWriter writer;

    @Before
    public void setup() throws Exception {
        processor = new RulesRecordProcessor();
        runner = TestRunners.newTestRunner(processor);
        parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.setProperty(RulesRecordProcessor.FACTS_RECORD_READER, "parser");
        writer = new JsonRecordSetWriter();
        runner.addControllerService("writer", writer);
        runner.enableControllerService(writer);
        runner.setProperty(RulesRecordProcessor.ACTION_RECORD_WRITER, "writer");
    }

    @Test
    public void testGetActions() throws Exception {
        Action action1 = new Action();
        Map<String,String> attributes1 = new HashMap<>();
        attributes1.put("testA","1");
        attributes1.put("testB","2");
        action1.setType("LOG");
        action1.setAttributes(attributes1);
        Action action2 = new Action();
        Map<String,String> attributes2 = new HashMap<>();
        attributes2.put("testC","1");
        attributes2.put("testD","2");
        action2.setType("ALERT");
        action2.setAttributes(attributes2);
        RulesEngineService rulesEngineService = new MockRulesEngineService(Lists.newArrayList(action1,action2));
        runner.addControllerService("MockRulesEngineService", rulesEngineService);
        runner.enableControllerService(rulesEngineService);
        runner.setProperty(RulesRecordProcessor.RULES_ENGINE,"MockRulesEngineService");
        generateTestData(1);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(RulesRecordProcessor.REL_ACTIONS, 1);
        runner.assertTransferCount(RulesRecordProcessor.REL_ORIGINAL, 1);
        final MockFlowFile action = runner.getFlowFilesForRelationship(RulesRecordProcessor.REL_ACTIONS).get(0);
        action.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
    }

    @Test
    public void testNoActionsReturned() throws Exception {
        RulesEngineService rulesEngineService = new MockRulesEngineService(Lists.newArrayList());
        runner.addControllerService("MockRulesEngineService", rulesEngineService);
        runner.enableControllerService(rulesEngineService);
        runner.setProperty(RulesRecordProcessor.RULES_ENGINE,"MockRulesEngineService");
        generateTestData(1);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(RulesRecordProcessor.REL_ACTIONS, 1);
        runner.assertTransferCount(RulesRecordProcessor.REL_ORIGINAL, 1);
        final MockFlowFile action = runner.getFlowFilesForRelationship(RulesRecordProcessor.REL_ACTIONS).get(0);
        action.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
    }

    @Test
    public void testNullActionsReturned() throws Exception {
        RulesEngineService rulesEngineService = new MockRulesEngineService(null);
        runner.addControllerService("MockRulesEngineService", rulesEngineService);
        runner.enableControllerService(rulesEngineService);
        runner.setProperty(RulesRecordProcessor.RULES_ENGINE,"MockRulesEngineService");
        generateTestData(1);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(RulesRecordProcessor.REL_ACTIONS, 1);
        runner.assertTransferCount(RulesRecordProcessor.REL_ORIGINAL, 1);
        final MockFlowFile action = runner.getFlowFilesForRelationship(RulesRecordProcessor.REL_ACTIONS).get(0);
        action.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
    }

    @Test
    public void testNoRecordsReceived() throws Exception {
        RulesEngineService rulesEngineService = new MockRulesEngineService(null);
        runner.addControllerService("MockRulesEngineService", rulesEngineService);
        runner.enableControllerService(rulesEngineService);
        runner.setProperty(RulesRecordProcessor.RULES_ENGINE,"MockRulesEngineService");
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(RulesRecordProcessor.REL_ACTIONS, 1);
        runner.assertTransferCount(RulesRecordProcessor.REL_ORIGINAL, 1);
        final MockFlowFile action = runner.getFlowFilesForRelationship(RulesRecordProcessor.REL_ACTIONS).get(0);
        action.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
    }

    private void generateTestData(int numRecords) {
        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("date", RecordFieldType.DATE);
        parser.addSchemaField("time", RecordFieldType.TIME);
        parser.addSchemaField("ts", RecordFieldType.TIMESTAMP);
        for(int i=1; i<=numRecords; i++) {
            parser.addRecord(i, "reç" + i, 100 + i, new Date(1545282000000L), new Time(68150000), new Timestamp(1545332150000L));
        }
    }

    public class MockRulesEngineService extends AbstractControllerService implements RulesEngineService {
        private List<Action> actions;

        public MockRulesEngineService(List<Action> actions) {
            this.actions = actions;
        }

        @Override
        public List<Action> fireRules(Map<String, Object> facts) {
            return actions;
        }

        @Override
        public String getIdentifier() {
            return "MockRulesEngineService";
        }
    }


}
