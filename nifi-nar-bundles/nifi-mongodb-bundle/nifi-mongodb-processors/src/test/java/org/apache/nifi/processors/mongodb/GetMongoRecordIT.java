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
package org.apache.nifi.processors.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.mongodb.MongoDBControllerService;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.StandardSchemaIdentifier;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetMongoRecordIT extends AbstractMongoDBIT {
    TestRunner runner;
    MongoDBClientService service;

    static RecordSchema SCHEMA;
    static final String DB_NAME = GetMongoRecord.class.getSimpleName() + Calendar.getInstance().getTimeInMillis();
    static final String COL_NAME = "test";

    static {
        List<RecordField> fields = Arrays.asList(
            new RecordField("name", RecordFieldType.STRING.getDataType()),
            new RecordField("failedLogins", RecordFieldType.INT.getDataType()),
            new RecordField("lastLogin", RecordFieldType.DATE.getDataType())
        );
        SCHEMA = new SimpleRecordSchema(fields, new StandardSchemaIdentifier.Builder().name("sample").build());
    }

    @BeforeEach
    void setup() throws Exception {
        runner = TestRunners.newTestRunner(GetMongoRecord.class);
        service = new MongoDBControllerService();
        runner.addControllerService("client", service);
        runner.setProperty(service, MongoDBControllerService.URI, mongoDBContainer.getConnectionString());
        runner.enableControllerService(service);

        JsonRecordSetWriter writer = new JsonRecordSetWriter();
        MockSchemaRegistry registry = new MockSchemaRegistry();
        registry.addSchema("sample", SCHEMA);

        runner.addControllerService("writer", writer);
        runner.addControllerService("registry", registry);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        runner.setProperty(writer, DateTimeUtils.DATE_FORMAT, "yyyy");
        runner.enableControllerService(registry);
        runner.enableControllerService(writer);

        runner.setProperty(GetMongoRecord.DATABASE_NAME, DB_NAME);
        runner.setProperty(GetMongoRecord.COLLECTION_NAME, COL_NAME);
        runner.setProperty(GetMongoRecord.CLIENT_SERVICE, "client");
        runner.setProperty(GetMongoRecord.WRITER_FACTORY, "writer");

        List<Map<String, Object>> SAMPLES = new ArrayList<>();
        Map<String, Object> temp = new HashMap<>();
        temp.put("name", "John Smith");
        temp.put("failedLogins", 2);
        temp.put("lastLogin", Calendar.getInstance().getTime());
        SAMPLES.add(temp);
        temp = new HashMap<>();
        temp.put("name", "Jane Doe");
        temp.put("failedLogins", 1);
        temp.put("lastLogin", new Date(Calendar.getInstance().getTime().getTime() - 360000));
        SAMPLES.add(temp);
        temp = new HashMap<>();
        temp.put("name", "John Brown");
        temp.put("failedLogins", 4);
        temp.put("lastLogin", new Date(Calendar.getInstance().getTime().getTime() - 10000));
        SAMPLES.add(temp);

        service.getDatabase(DB_NAME).getCollection(COL_NAME)
                .insertMany(SAMPLES.stream().map(it -> new Document(it)).collect(Collectors.toList()));
    }

    @AfterEach
    void after() {
        service.getDatabase(DB_NAME).drop();
    }

    @Test
    void testSortAndProjection() throws Exception {
        runner.setIncomingConnection(false);
        runner.setVariable("schema.name", "sample");
        runner.setProperty(GetMongoRecord.SORT, new Document("failedLogins",  1).toJson());
        runner.setProperty(GetMongoRecord.PROJECTION, new Document("failedLogins",  1).toJson());
        runner.setProperty(GetMongoRecord.QUERY, "{}");
        runner.run();

        List<Map<String, Object>> parsed = sharedTest();
        assertEquals(3, parsed.size());
        List<Integer> values = Arrays.asList(1, 2, 4);
        int index = 0;
        for (Map<String, Object> it : parsed) {
            assertEquals(values.get(index++), it.get("failedLogins"));
            assertNull(it.get("name"));
            assertNull(it.get("lastLogin"));
        }
    }

    private static interface LookupValidator {
        void validate(TestRunner runner);
    }

    @Test
    void testLookup() throws Exception {
        LookupValidator ffValidator =  (runner) -> {
            List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(GetMongoRecord.REL_SUCCESS);
            assertNotNull(ffs);
            assertTrue(ffs.size() == 1);
            assertEquals("3", ffs.get(0).getAttribute("record.count"));
            assertEquals("application/json", ffs.get(0).getAttribute(CoreAttributes.MIME_TYPE.key()));
            assertEquals(COL_NAME, ffs.get(0).getAttribute(GetMongoRecord.COL_NAME));
            assertEquals(DB_NAME, ffs.get(0).getAttribute(GetMongoRecord.DB_NAME));
            assertEquals(Document.parse("{}"), Document.parse(ffs.get(0).getAttribute("executed.query")));
        };

        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.name", "sample");

        runner.setProperty(GetMongoRecord.QUERY_ATTRIBUTE, "executed.query");
        runner.setProperty(GetMongoRecord.QUERY, "{}");
        runner.enqueue("", attrs);
        runner.run();

        runner.assertTransferCount(GetMongoRecord.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongoRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(GetMongoRecord.REL_ORIGINAL, 1);

        ffValidator.validate(runner);

        runner.clearTransferState();
        runner.removeProperty(GetMongoRecord.QUERY);
        runner.enqueue("{}", attrs);
        runner.run();

        runner.assertTransferCount(GetMongoRecord.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongoRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(GetMongoRecord.REL_ORIGINAL, 1);

        ffValidator.validate(runner);
    }

    @Test
    void testLimit() throws Exception {
        runner.setIncomingConnection(false);
        runner.setProperty(GetMongoRecord.LIMIT, "1");
        runner.setProperty(GetMongoRecord.QUERY, "{}");
        runner.setVariable("schema.name", "sample");
        runner.run();

        List<Map<String, Object>> parsed = sharedTest();
        assertEquals(1, parsed.size());
    }

    List<Map<String, Object>> sharedTest() throws Exception {
        runner.assertTransferCount(GetMongoRecord.REL_FAILURE, 0);
        runner.assertTransferCount(GetMongoRecord.REL_SUCCESS, 1);

        MockFlowFile ff = runner.getFlowFilesForRelationship(GetMongoRecord.REL_SUCCESS).get(0);
        byte[] raw = runner.getContentAsByteArray(ff);
        String content = new String(raw);
        List<Map<String, Object>> parsed = new ObjectMapper().readValue(content, List.class);
        assertNotNull(parsed);
        return parsed;
    }
}
