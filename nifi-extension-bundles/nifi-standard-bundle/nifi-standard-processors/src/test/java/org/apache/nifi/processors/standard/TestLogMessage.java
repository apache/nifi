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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLogMessage {

    private TestRunner runner;

    @BeforeEach
    void before() throws InitializationException {
        runner = TestRunners.newTestRunner(LogMessage.class);
    }

    @AfterEach
    void after() throws InitializationException {
        runner.shutdown();
    }

    @Test
    void testInfoMessageLogged() {

        runner.setProperty(LogMessage.LOG_MESSAGE, "This should help the operator to follow the flow: ${foobar}");
        runner.setProperty(LogMessage.LOG_LEVEL, LogMessage.MessageLogLevel.info.toString());
        Map<String, String> flowAttributes = new HashMap<>();
        flowAttributes.put("foobar", "baz");

        runner.enqueue("This is a message!", flowAttributes);
        runner.run();

        List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(LogMessage.REL_SUCCESS);
        assertEquals(1, successFlowFiles.size());

        MockComponentLog mockComponentLog = runner.getLogger();

        assertFalse(mockComponentLog.getInfoMessages().isEmpty());
        assertTrue(mockComponentLog.getTraceMessages().isEmpty());
        assertTrue(mockComponentLog.getDebugMessages().isEmpty());
        assertTrue(mockComponentLog.getWarnMessages().isEmpty());
        assertTrue(mockComponentLog.getErrorMessages().isEmpty());
    }

    @Test
    void testInfoMessageWithPrefixLogged() {

        runner.setProperty(LogMessage.LOG_PREFIX, "FOOBAR>>>");
        runner.setProperty(LogMessage.LOG_MESSAGE, "This should help the operator to follow the flow: ${foobar}");
        runner.setProperty(LogMessage.LOG_LEVEL, LogMessage.MessageLogLevel.info.toString());

        Map<String, String> flowAttributes = new HashMap<>();
        flowAttributes.put("foobar", "baz");

        runner.enqueue("This is a message!", flowAttributes);
        runner.run();

        List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(LogMessage.REL_SUCCESS);
        assertEquals(1, successFlowFiles.size());

        MockComponentLog mockComponentLog = runner.getLogger();

        assertFalse(mockComponentLog.getInfoMessages().isEmpty());
        assertTrue(mockComponentLog.getTraceMessages().isEmpty());
        assertTrue(mockComponentLog.getDebugMessages().isEmpty());
        assertTrue(mockComponentLog.getWarnMessages().isEmpty());
        assertTrue(mockComponentLog.getErrorMessages().isEmpty());
    }

    @Test
    void testInvalidLogLevel() {
        runner.setProperty(LogMessage.LOG_LEVEL, "whatever");
        runner.assertNotValid();
    }

    @ParameterizedTest
    @EnumSource(LogMessage.MessageLogLevel.class)
    void testLogLevelCaseInsensitivity(LogMessage.MessageLogLevel logLevel) {
        runner.setProperty(LogMessage.LOG_LEVEL, logLevel.name().toUpperCase());
        runner.assertValid();
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("log-level", LogMessage.LOG_LEVEL.getName()),
                Map.entry("log-prefix", LogMessage.LOG_PREFIX.getName()),
                Map.entry("log-message", LogMessage.LOG_MESSAGE.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }
}
