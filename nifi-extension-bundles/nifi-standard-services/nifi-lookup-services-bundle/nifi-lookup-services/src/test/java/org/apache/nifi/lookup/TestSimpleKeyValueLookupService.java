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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSimpleKeyValueLookupService {

    @Test
    public void testSimpleKeyValueLookupService() throws InitializationException {
        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();

        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.setProperty(service, "key1", "value1");
        runner.setProperty(service, "key2", "value2");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final Optional<String> get1 = service.lookup(Collections.singletonMap("key", "key1"));
        assertEquals(Optional.of("value1"), get1);

        final Optional<String> get2 = service.lookup(Collections.singletonMap("key", "key2"));
        assertEquals(Optional.of("value2"), get2);

        final Optional<String> get3 = service.lookup(Collections.singletonMap("key", "key3"));
        assertTrue(get3.isEmpty());
    }
}