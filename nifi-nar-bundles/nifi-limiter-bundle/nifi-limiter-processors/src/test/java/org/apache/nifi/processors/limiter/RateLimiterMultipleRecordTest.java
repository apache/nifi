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
import org.junit.Ignore;
import org.junit.Test;

public class RateLimiterMultipleRecordTest {

    private TestRunner runner;
    //private ExternalHazelcastCacheManager testSubject;

    @Before
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(new RateMultipleLimiterRecord());
        ControllerService reader = new JsonTreeReader();
        ControllerService writer = new JsonRecordSetWriter();
        ControllerService registry = new MockSchemaRegistry();
        runner.addControllerService("reader", reader);
        runner.addControllerService("writer", writer);
        runner.addControllerService("registry", registry);



        try (InputStream is = getClass().getResourceAsStream("/avro/record_multi-limiter_schema.avsc")) {
            String raw = IOUtils.toString(is, "UTF-8");
            RecordSchema parsed = AvroTypeUtil.createSchema(new Schema.Parser().parse(raw));
            ((MockSchemaRegistry) registry).addSchema("record", parsed);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        runner.setProperty(RateMultipleLimiterRecord.RECORD_READER, "reader");
        runner.setProperty(RateMultipleLimiterRecord.RECORD_WRITER, "writer");
        runner.setProperty(RateMultipleLimiterRecord.KEY, "unknown_key");
        runner.setProperty(RateMultipleLimiterRecord.KEY_REMOVED, "false");
        runner.setProperty(RateMultipleLimiterRecord.KEY_RECORD_PATH, "/name");
        runner.setProperty(RateMultipleLimiterRecord.REFILL_TOKENS, "1000");
        runner.setProperty(RateMultipleLimiterRecord.REFILL_TOKENS_RECORD_PATH, "/bucket/refill_token");
        runner.setProperty(RateMultipleLimiterRecord.REFILL_TOKENS_REMOVED, "false");
        runner.setProperty(RateMultipleLimiterRecord.REFILL_PERIOD, "10 sec");
        runner.setProperty(RateMultipleLimiterRecord.REFILL_PERIOD_RECORD_PATH, "/bucket/refill_period");
        runner.setProperty(RateMultipleLimiterRecord.REFILL_PERIOD_REMOVED, "false");
        runner.setProperty(RateMultipleLimiterRecord.BANDWIDTH_CAPACITY,  "10000");
        runner.setProperty(RateMultipleLimiterRecord.BANDWIDTH_CAPACITY_RECORD_PATH, "/bucket/bandwidth");
        runner.setProperty(RateMultipleLimiterRecord.BANDWIDTH_CAPACITY_REMOVED, "false");
        runner.setProperty(RateMultipleLimiterRecord.MAX_SIZE_BUCKET,  "500000");
        runner.setProperty(RateMultipleLimiterRecord.EXPIRE_DURATION,  "60 min");
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

        runner.assertTransferCount(RateMultipleLimiterRecord.REL_FLOODING, flood);
        runner.assertTransferCount(RateMultipleLimiterRecord.REL_SUCCESS, ok);
        runner.assertTransferCount(RateMultipleLimiterRecord.REL_FAILURE, fail);
    }

    @Test
    @Ignore
    public void testRateMultiLimiterSuccess() throws Exception {
        commonTest("/json/record_multi-limiter.json", 1, 1, 0);
    }


} 
