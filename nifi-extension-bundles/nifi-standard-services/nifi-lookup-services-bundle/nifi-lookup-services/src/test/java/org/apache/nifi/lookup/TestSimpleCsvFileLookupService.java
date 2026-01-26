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
package org.apache.nifi.lookup;

import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSimpleCsvFileLookupService {

    private TestRunner runner;
    private SimpleCsvFileLookupService service;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new SimpleCsvFileLookupService();
        runner.addControllerService("csv-file-lookup-service", service);
    }

    @Test
    public void testSimpleCsvFileLookupService() throws LookupFailureException {
        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FILE, "src/test/resources/test.csv");
        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FORMAT, "RFC4180");
        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_KEY_COLUMN, "key");
        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_VALUE_COLUMN, "value");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final SimpleCsvFileLookupService lookupService =
                (SimpleCsvFileLookupService) runner.getProcessContext()
                        .getControllerServiceLookup()
                        .getControllerService("csv-file-lookup-service");

        final Optional<String> property1 = lookupService.lookup(Collections.singletonMap("key", "property.1"));
        assertEquals(Optional.of("this is property 1"), property1);

        final Optional<String> property2 = lookupService.lookup(Collections.singletonMap("key", "property.2"));
        assertEquals(Optional.of("this is property 2"), property2);

        final Optional<String> property3 = lookupService.lookup(Collections.singletonMap("key", "property.3"));
        assertEquals(Optional.empty(), property3);
    }

    @Test
    public void testSimpleCsvFileLookupServiceWithCharset() throws LookupFailureException {
        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FILE, "src/test/resources/test_Windows-31J.csv");
        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FORMAT, "RFC4180");
        runner.setProperty(service, SimpleCsvFileLookupService.CHARSET, "Windows-31J");
        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_KEY_COLUMN, "key");
        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_VALUE_COLUMN, "value");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final Optional<String> property1 = service.lookup(Collections.singletonMap("key", "property.1"));
        assertTrue(property1.isPresent());
        assertEquals("this is property \uff11", property1.get());
    }

    @Test
    public void testSimpleCsvFileLookupServiceWithCustomSeparatorQuotedEscaped() throws LookupFailureException {
        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FORMAT, "custom");
        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FILE, "src/test/resources/test_sep_escape_comment.csv");
        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_KEY_COLUMN, "key");
        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_VALUE_COLUMN, "value");
        runner.setProperty(service, CSVUtils.VALUE_SEPARATOR, "|");
        runner.setProperty(service, CSVUtils.QUOTE_CHAR, "\"");
        runner.setProperty(service, CSVUtils.ESCAPE_CHAR, "%");
        runner.setProperty(service, CSVUtils.COMMENT_MARKER, "#");
        runner.setProperty(service, CSVUtils.QUOTE_MODE, CSVUtils.QUOTE_ALL);
        runner.enableControllerService(service);
        runner.assertValid(service);

        final Optional<String> value = service.lookup(Collections.singletonMap("key", "my_key"));
        assertEquals(Optional.of("my_value with an escaped |."), value);
    }

    @Test
    public void testCacheIsClearedWhenDisableService() {
        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FILE, "src/test/resources/test.csv");
        runner.setProperty(service, SimpleCsvFileLookupService.CSV_FORMAT, "RFC4180");
        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_KEY_COLUMN, "key");
        runner.setProperty(service, SimpleCsvFileLookupService.LOOKUP_VALUE_COLUMN, "value");
        runner.enableControllerService(service);
        runner.assertValid(service);

        assertTrue(service.isCaching());

        runner.disableControllerService(service);

        assertFalse(service.isCaching());
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("lookup-value-column", SimpleCsvFileLookupService.LOOKUP_VALUE_COLUMN.getName()),
                Map.entry("csv-file", AbstractCSVLookupService.CSV_FILE.getName()),
                Map.entry("lookup-key-column", AbstractCSVLookupService.LOOKUP_KEY_COLUMN.getName()),
                Map.entry("ignore-duplicates", AbstractCSVLookupService.IGNORE_DUPLICATES.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);
    }
}
