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

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestUpdateCounter {

    private static final String COUNTER_NAME = "firewall";
    private static final String DELTA = "1";

    private static final String FILENAME_ATTRIBUTE = "filename";
    private static final String FILENAME_VALUE = "test";

    private static final String NUM_ATTRIBUTE = "num";
    private static final String NUM_ATTRIBUTES_VALUE = "5";

    private static final String EMPTY_ATTRIBUTE = "emptyAttribute";
    private static final String EMPTY_VALUE = "emptyValue";

    private static final String LEGACY_COUNTER_NAME_PROPERTY = "counter-name";
    private static final String LEGACY_DELTA_PROPERTY = "delta";

    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(new UpdateCounter());
    }

    @Test
    public void testBaseScenario() {
        runner.setProperty(UpdateCounter.COUNTER_NAME, COUNTER_NAME);
        runner.setProperty(UpdateCounter.DELTA, DELTA);

        runner.enqueue(new byte[0], emptyMap());
        runner.run();

        final Long counterValue = runner.getCounterValue(COUNTER_NAME);
        assertEquals(counterValue, Long.valueOf(DELTA));
        runner.assertAllFlowFilesTransferred(UpdateCounter.SUCCESS, 1);
    }

    @Test
    public void testExpressionLanguage() {
        runner.setProperty(UpdateCounter.COUNTER_NAME, "${%s}".formatted(FILENAME_ATTRIBUTE));
        runner.setProperty(UpdateCounter.DELTA, "${%s}".formatted(NUM_ATTRIBUTE));

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(FILENAME_ATTRIBUTE, FILENAME_VALUE);
        attributes.put(NUM_ATTRIBUTE, NUM_ATTRIBUTES_VALUE);

        runner.enqueue(new byte[0], attributes);
        runner.run();

        final Long counterValue = runner.getCounterValue(FILENAME_VALUE);
        assertEquals(counterValue, Long.valueOf(NUM_ATTRIBUTES_VALUE));
        runner.assertAllFlowFilesTransferred(UpdateCounter.SUCCESS, 1);
    }

    @Test
    public void testAttributes() {
        runner.setProperty(UpdateCounter.COUNTER_NAME, COUNTER_NAME);
        runner.setProperty(UpdateCounter.DELTA, DELTA);
        runner.setProperty(FILENAME_ATTRIBUTE, "${%s}".formatted(FILENAME_ATTRIBUTE));
        runner.setProperty(EMPTY_ATTRIBUTE, "${%s}".formatted(EMPTY_VALUE));
        runner.setProperty(NUM_ATTRIBUTE, NUM_ATTRIBUTES_VALUE);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(FILENAME_ATTRIBUTE, FILENAME_VALUE);

        runner.enqueue(new byte[0], attributes);
        runner.run();

        final Long counterValue = runner.getCounterValue(COUNTER_NAME, Map.of(FILENAME_ATTRIBUTE, FILENAME_VALUE, NUM_ATTRIBUTE, NUM_ATTRIBUTES_VALUE));
        assertEquals(Long.valueOf(DELTA), counterValue);
        assertNull(runner.getCounterValue(COUNTER_NAME, Map.of()));
        runner.assertAllFlowFilesTransferred(UpdateCounter.SUCCESS, 1);
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.of(
                LEGACY_COUNTER_NAME_PROPERTY, UpdateCounter.COUNTER_NAME.getName(),
                LEGACY_DELTA_PROPERTY, UpdateCounter.DELTA.getName()
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }
}
