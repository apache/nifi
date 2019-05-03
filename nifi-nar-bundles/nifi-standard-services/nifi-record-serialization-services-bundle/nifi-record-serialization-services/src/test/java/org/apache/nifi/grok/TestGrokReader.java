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
package org.apache.nifi.grok;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class TestGrokReader {

    private String originalHome = "";

    @Before
    public void beforeEach() {
        originalHome = System.getProperty("user.home");
    }

    @After
    public void afterEach() {
        System.setProperty("user.home", originalHome);
    }

    @Test
    public void testSetupOfGrokReaderService() throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(TestRegistryProcessor.class);
        final GrokReader reader = new GrokReader();

        runner.addControllerService("grok-schema-registry-service", reader);

        runner.setProperty(reader, GrokReader.PATTERN_FILE, "src/test/resources/grok/grok-pattern-file");
        runner.setProperty(reader, GrokReader.GROK_EXPRESSION, "%{GREEDYDATA:message}");
        runner.setProperty(reader, GrokReader.NO_MATCH_BEHAVIOR, GrokReader.SKIP_LINE);
        runner.enableControllerService(reader);
        runner.assertValid(reader);

        final GrokReader readerService =
                (GrokReader) runner.getProcessContext()
                        .getControllerServiceLookup()
                          .getControllerService("grok-schema-registry-service");

        assertThat(readerService, instanceOf(SchemaRegistryService.class));
    }

    @Test
    public void testSetupOfGrokReaderServiceHonorsPathExpansion() throws InitializationException {
        System.setProperty("user.home", "src/test/resources/grok");

        final TestRunner runner = TestRunners.newTestRunner(TestRegistryProcessor.class);
        final GrokReader reader = new GrokReader();

        runner.addControllerService("grok-schema-registry-service", reader);

        runner.setProperty(reader, GrokReader.PATTERN_FILE, "~/grok-pattern-file");
        runner.setProperty(reader, GrokReader.GROK_EXPRESSION, "%{GREEDYDATA:message}");
        runner.setProperty(reader, GrokReader.NO_MATCH_BEHAVIOR, GrokReader.SKIP_LINE);
        runner.enableControllerService(reader);
        runner.assertValid(reader);

        final GrokReader readerService =
                (GrokReader) runner.getProcessContext()
                        .getControllerServiceLookup()
                        .getControllerService("grok-schema-registry-service");

        assertThat(readerService, instanceOf(SchemaRegistryService.class));
    }

}
