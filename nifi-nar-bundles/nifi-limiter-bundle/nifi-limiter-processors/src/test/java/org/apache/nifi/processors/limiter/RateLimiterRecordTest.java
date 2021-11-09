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
package org.apache.nifi.processors.limiter;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Before;
import org.junit.Test;



public class RateLimiterRecordTest {

    private TestRunner runner;

    @Before
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(new RateLimiterRecord());
        ControllerService reader = new JsonTreeReader();
        ControllerService writer = new JsonRecordSetWriter();
        ControllerService registry = new MockSchemaRegistry();
        runner.addControllerService("reader", reader);
        runner.addControllerService("writer", writer);
        runner.addControllerService("registry", registry);

        try (InputStream is = getClass().getResourceAsStream("/avro/record_limiter_schema.avsc")) {
            String raw = IOUtils.toString(is, "UTF-8");
            RecordSchema parsed = AvroTypeUtil.createSchema(new Schema.Parser().parse(raw));
            ((MockSchemaRegistry) registry).addSchema("record", parsed);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        runner.setProperty(RateLimiterRecord.RECORD_READER, "reader");
        runner.setProperty(RateLimiterRecord.RECORD_WRITER, "writer");
        runner.setProperty(RateLimiterRecord.KEY, "unknown_key");
        runner.setProperty(RateLimiterRecord.KEY_REMOVED, "false");
        runner.setProperty(RateLimiterRecord.KEY_RECORD_PATH, "/name");
        runner.setProperty(RateLimiterRecord.REFILL_TOKENS, "1000");
        runner.setProperty(RateLimiterRecord.REFILL_PERIOD, "10 sec");
        runner.setProperty(RateLimiterRecord.BANDWIDTH_CAPACITY,  "10000");
        runner.setProperty(RateLimiterRecord.MAX_SIZE_BUCKET,  "500000");
        runner.setProperty(RateLimiterRecord.EXPIRE_DURATION,  "60 min");
        runner.enableControllerService(registry);
        runner.enableControllerService(reader);
        runner.enableControllerService(writer);
        runner.assertValid();

    }

    private void commonTest(String path, int flood, int ok, int fail) throws IOException {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.name", "record");
        runner.enqueue(getClass().getResourceAsStream(path), attrs);
        runner.run();

        runner.assertTransferCount(RateLimiterRecord.REL_FLOODING, flood);
        runner.assertTransferCount(RateLimiterRecord.REL_SUCCESS, ok);
        runner.assertTransferCount(RateLimiterRecord.REL_FAILURE, fail);
    }

    @Test
    public void testRateLimiterSuccess() throws Exception {
        commonTest("/json/record_with_key.json", 0, 1, 0);
    }

    @Test
    public void testRateLimiterFlooding() throws Exception {
        runner.setProperty(RateLimiterRecord.BANDWIDTH_CAPACITY,  "1");
        commonTest("/json/record_with_same_key.json", 1, 1, 0);
    }

    @Test
    public void testRateLimiterWithNoKey() throws Exception {
        commonTest("/json/record_with_nokey.json", 0, 1, 0);
    }

    @Test
    public void testRateLimiterWithNoKeyNested() throws Exception {
        runner.setProperty(RateLimiterRecord.KEY_RECORD_PATH, "/host/id");
        commonTest("/json/record_with_nokey.json", 0, 1, 0);
    }

    @Test
    public void testRateLimiterWithNoKeyNFlooding() throws Exception {
        runner.setProperty(RateLimiterRecord.KEY_RECORD_PATH, "/name");
        runner.setProperty(RateLimiterRecord.BANDWIDTH_CAPACITY,  "1");
        commonTest("/json/record_with_nokey.json", 1, 1, 0);
    }

} 
