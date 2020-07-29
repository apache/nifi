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

import java.util.Collections;
import java.util.Optional;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TestPropertiesFileLookupService {

    final static Optional<String> EMPTY_STRING = Optional.empty();

    @Test
    public void testPropertiesFileLookupService() throws InitializationException, LookupFailureException {
        final TestRunner runner = TestRunners.newTestRunner(TestProcessor.class);
        final PropertiesFileLookupService service = new PropertiesFileLookupService();

        runner.addControllerService("properties-file-lookup-service", service);
        runner.setProperty(service, PropertiesFileLookupService.CONFIGURATION_FILE, "src/test/resources/test.properties");
        runner.enableControllerService(service);
        runner.assertValid(service);

        final PropertiesFileLookupService lookupService =
            (PropertiesFileLookupService) runner.getProcessContext()
                .getControllerServiceLookup()
                .getControllerService("properties-file-lookup-service");

        assertThat(lookupService, instanceOf(LookupService.class));

        final Optional<String> property1 = lookupService.lookup(Collections.singletonMap("key", "property.1"));
        assertEquals(Optional.of("this is property 1"), property1);

        final Optional<String> property2 = lookupService.lookup(Collections.singletonMap("key", "property.2"));
        assertEquals(Optional.of("this is property 2"), property2);

        final Optional<String> property3 = lookupService.lookup(Collections.singletonMap("key", "property.3"));
        assertEquals(EMPTY_STRING, property3);
    }

}
