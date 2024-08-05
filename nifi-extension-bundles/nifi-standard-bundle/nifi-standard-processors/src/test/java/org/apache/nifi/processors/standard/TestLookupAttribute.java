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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.lookup.SimpleKeyValueLookupService;
import org.apache.nifi.lookup.StringLookupService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLookupAttribute {

    @Test
    public void testKeyValueLookupAttribute() throws InitializationException {
        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.setProperty(service, "key1", "value1");
        runner.setProperty(service, "key2", "value2");
        runner.setProperty(service, "key3", "value3");
        runner.setProperty(service, "key4", "  ");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "simple-key-value-lookup-service");
        runner.setProperty(LookupAttribute.INCLUDE_EMPTY_VALUES, "true");
        runner.setProperty("foo", "key1");
        runner.setProperty("bar", "key2");
        runner.setProperty("baz", "${attr1}");
        runner.setProperty("qux", "key4");
        runner.setProperty("zab", "key5");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr1", "key3");

        runner.enqueue("some content".getBytes(), attributes);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(LookupAttribute.REL_MATCHED, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_MATCHED).getFirst();

        assertNotNull(flowFile);

        flowFile.assertAttributeExists("foo");
        flowFile.assertAttributeExists("bar");
        flowFile.assertAttributeExists("baz");
        flowFile.assertAttributeExists("qux");
        flowFile.assertAttributeExists("zab");
        flowFile.assertAttributeNotExists("zar");

        flowFile.assertAttributeEquals("foo", "value1");
        flowFile.assertAttributeEquals("bar", "value2");
        flowFile.assertAttributeEquals("baz", "value3");
        flowFile.assertAttributeEquals("qux", "");
        flowFile.assertAttributeEquals("zab", "null");
    }

    @Test
    public void testLookupAttributeUnmatched() throws InitializationException {
        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.setProperty(service, "key1", "value1");
        runner.setProperty(service, "key2", "value2");
        runner.setProperty(service, "key3", "value3");
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "simple-key-value-lookup-service");
        runner.setProperty(LookupAttribute.INCLUDE_EMPTY_VALUES, "false");
        runner.setProperty("baz", "${attr1}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("attr1", "key4");

        runner.enqueue("some content".getBytes(), attributes);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(LookupAttribute.REL_UNMATCHED, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(LookupAttribute.REL_UNMATCHED).getFirst();

        assertNotNull(flowFile);

        flowFile.assertAttributeExists("attr1");
        flowFile.assertAttributeNotExists("baz");
        flowFile.assertAttributeEquals("attr1", "key4");
    }

    @Test
    public void testCustomValidateInvalidLookupService() throws InitializationException {
        final InvalidLookupService service = new InvalidLookupService();

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("invalid-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "invalid-lookup-service");
        runner.setProperty("foo", "key1");
        runner.assertNotValid();
    }

    @Test
    public void testCustomValidateMissingDynamicProps() throws InitializationException {
        final SimpleKeyValueLookupService service = new SimpleKeyValueLookupService();

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "simple-key-value-lookup-service");
        runner.assertNotValid();
    }

    @Test
    public void testLookupServicePassFlowfileAttributes() throws InitializationException {
        final LookupService service = new TestService();

        final TestRunner runner = TestRunners.newTestRunner(new LookupAttribute());
        runner.addControllerService("simple-key-value-lookup-service", service);
        runner.enableControllerService(service);
        runner.assertValid(service);
        runner.setProperty(LookupAttribute.LOOKUP_SERVICE, "simple-key-value-lookup-service");
        runner.setProperty(LookupAttribute.INCLUDE_EMPTY_VALUES, "false");
        runner.setProperty("baz", "${attr1}");
        runner.assertValid();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("user_defined", "key4");

        runner.enqueue("some content".getBytes(), attributes);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(LookupAttribute.REL_MATCHED, 1);
    }

    private static class InvalidLookupService extends AbstractControllerService implements StringLookupService {
      @Override
      public Optional<String> lookup(Map<String, Object> coordinates) {
          return Optional.empty();
      }

      @Override
      public Set<String> getRequiredKeys() {
          return Set.of("key1", "key2");
      }
    }

    static class TestService extends AbstractControllerService implements StringLookupService {
        @Override
        public Optional<String> lookup(Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
            assertNotNull(coordinates);
            assertNotNull(context);
            assertEquals(1, coordinates.size());
            assertTrue(context.containsKey("user_defined"));

            return Optional.of("Test!");
        }

        @Override
        public Optional<String> lookup(Map<String, Object> coordinates) {
            return Optional.empty();
        }

        @Override
        public Class<?> getValueType() {
            return String.class;
        }

        @Override
        public Set<String> getRequiredKeys() {
            return Set.of("key");
        }
    }
}
