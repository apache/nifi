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
package org.apache.nifi.processors.standard;

import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUpdateCounter {
    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(new UpdateCounter());
    }

    @Test
    public void testwithFileName() {
        runner.setProperty(UpdateCounter.COUNTER_NAME, "firewall");
        runner.setProperty(UpdateCounter.DELTA, "1");
        Map<String, String> attributes = new HashMap<>();
        runner.enqueue("", attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateCounter.SUCCESS, 1);
    }

    @Test
    public void testExpressionLanguage() {
        runner.setProperty(UpdateCounter.COUNTER_NAME, "${filename}");
        runner.setProperty(UpdateCounter.DELTA, "${num}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "test");
        attributes.put("num", "40");

        runner.enqueue(new byte[0], attributes);
        runner.run();
        Long counter = runner.getCounterValue("test");
        assertEquals(java.util.Optional.ofNullable(counter), java.util.Optional.of(40L));
        runner.assertAllFlowFilesTransferred(UpdateCounter.SUCCESS, 1);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.of(
                "counter-name", UpdateCounter.COUNTER_NAME.getName(),
                "delta", UpdateCounter.DELTA.getName()
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }
}
