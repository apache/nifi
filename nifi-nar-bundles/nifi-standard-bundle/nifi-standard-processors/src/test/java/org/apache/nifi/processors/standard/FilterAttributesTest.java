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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FilterAttributesTest {

    private final TestRunner runner = TestRunners.newTestRunner(FilterAttributes.class);

    private final String exampleContent = "lorem ipsum dolor sit amet";

    private final Map<String, String> exampleAttributes = Map.of(
            "foo", "fooValue",
            "bar", "barValue",
            "batz", "batzValue"
    );
    private final String exampleDelimiter = ",";

    @Nested
    class InModeRetain {
        @Test
        void retainsAllAttributesWhenAllAreFiltered() {
            final String attributeSet = "foo,bar,batz";
            final Set<String> expectedAttributes = Set.of("foo", "bar", "batz", "uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }

        @Test
        void retainsUUIDAndFilteredAttributesWhenOnlySomeAreFiltered() {
            final String attributeSet = "bar";
            final Set<String> expectedAttributes = Set.of("bar", "uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }

        @Test
        void retainsUUIDOnlyWhenNoneOfTheAttributesAreFiltered() {
            final String attributeSet = "other";
            final Set<String> expectedAttributes = Set.of("uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }
    }

    @Nested
    class InModeRemove {

        @BeforeEach
        void setUp() {
            runner.setProperty(FilterAttributes.FILTER_MODE, FilterAttributes.FILTER_MODE_VALUE_REMOVE);
        }

        @Test
        void removesAllAttributesExceptUUIDWhenAllAreFiltered() {
            final String attributeSet = "foo,bar,batz,uuid,path,filename";
            final Set<String> expectedAttributes = Set.of("uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }

        @Test
        void removesFilteredAttributesExceptUUIDWhenOnlySomeAreFiltered() {
            final String attributeSet = "bar,uuid,path,filename";
            final Set<String> expectedAttributes = Set.of("foo", "batz", "uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }

        @Test
        void removesNoAttributeWhenNoneOfTheAttributesAreFiltered() {
            final String attributeSet = "other";
            final Set<String> expectedAttributes = Set.of("foo", "bar", "batz", "uuid", "path", "filename");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }
    }

    @Nested
    class RegardingAttributeSetParsing {

        @Test
        void ignoresLeadingDelimiters() {
            final String attributeSet = ",foo,bar";
            final Set<String> expectedAttributes = Set.of("foo", "bar", "uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }

        @Test
        void ignoresTrailingDelimiters() {
            final String attributeSet = "foo,bar,";
            final Set<String> expectedAttributes = Set.of("foo", "bar", "uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }

        @Test
        void doesNotYieldErrorWhenAttributeSetIsEffectivelyEmpty() {
            final String attributeSet = " , ";
            final Set<String> expectedAttributes = Set.of("uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }

        @Test
        void worksWithSingleAttributeInSet() {
            final String attributeSet = "batz";
            final Set<String> expectedAttributes = Set.of("batz", "uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }

        @Test
        void worksWithMultipleAttributesInSet() {
            final String attributeSet = "foo,bar,batz";
            final Set<String> expectedAttributes = Set.of("foo", "bar", "batz", "uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }

        @Test
        void ignoresLeadingWhitespaceInAttributeName() {
            final String attributeSet = "foo,  batz";
            final Set<String> expectedAttributes = Set.of("foo", "batz", "uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }

        @Test
        void ignoresTrailingWhitespaceInAttributeName() {
            final String attributeSet = "foo  ,bar";
            final Set<String> expectedAttributes = Set.of("foo", "bar", "uuid");

            runTestWith(exampleAttributes, attributeSet, exampleDelimiter, expectedAttributes);
        }
    }

    @Test
    void supportsDefiningCustomDelimiter() {
        final String attributeSet = "bar:batz";
        final String delimiter = ":";
        final Set<String> expectedAttributes = Set.of("bar", "batz", "uuid");

        runTestWith(exampleAttributes, attributeSet, delimiter, expectedAttributes);
    }

    @Test
    void supportsDefiningCustomMultiCharacterDelimiter() {
        final String attributeSet = "barDELI;MITERbatz";
        final String delimiter = "DELI;MITER";
        final Set<String> expectedAttributes = Set.of("bar", "batz", "uuid");

        runTestWith(exampleAttributes, attributeSet, delimiter, expectedAttributes);
    }

    @Test
    void supportsDefiningAttributesToKeepInFlowFileAttribute() {
        final Map<String, String> attributes = new HashMap<>(exampleAttributes);
        attributes.put("lookup", "bar,batz");
        final String attributeSet = "${lookup}"; // NiFi EL with reference to FlowFile attribute
        final Set<String> expectedAttributes = Set.of("bar", "batz", "uuid");

        runTestWith(attributes, attributeSet, exampleDelimiter, expectedAttributes);
    }

    @Test
    void supportsDefiningDelimiterInFlowFileAttribute() {
        final Map<String, String> attributes = new HashMap<>(exampleAttributes);
        attributes.put("lookup", ",");
        final String delimiter = "${lookup}"; // NiFi EL with reference to FlowFile attribute
        final String attributeSet = "bar,batz";
        final Set<String> expectedAttributes = Set.of("bar", "batz", "uuid");

        runTestWith(attributes, attributeSet, delimiter, expectedAttributes);
    }

    @Test
    void supportMultiThreadedExecution() {
        runner.setThreadCount(5);

        final int flowFileCount = 10_000;
        for (int i = 0; i < flowFileCount; i++) {
            runner.enqueue(exampleContent, Map.of(
                    "foo", "" + i,
                    "bar", "" + i
            ));
        }
        runner.setProperty(FilterAttributes.ATTRIBUTE_SET, "foo");

        runner.run(flowFileCount);
        runner.assertAllFlowFilesTransferred(FilterAttributes.REL_SUCCESS, flowFileCount);
        List<MockFlowFile> resultFlowFiles = runner.getFlowFilesForRelationship(FilterAttributes.REL_SUCCESS);
        for (final MockFlowFile resultFlowFile : resultFlowFiles) {
            resultFlowFile.assertAttributeExists("foo");
            resultFlowFile.assertAttributeNotExists("bar");
        }
        final Set<String> fooValues = resultFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("foo"))
                .collect(Collectors.toUnmodifiableSet());
        assertEquals(flowFileCount, fooValues.size());
    }

    private void runTestWith(
            Map<String, String> attributes,
            String attributeSet,
            String delimiter,
            Set<String> expectedAttributes
    ) {
        runner.setProperty(FilterAttributes.ATTRIBUTE_SET, attributeSet);
        runner.setProperty(FilterAttributes.DELIMITER, delimiter);

        final MockFlowFile input = runner.enqueue(exampleContent, attributes);
        final Map<String, String> inputAttributes = input.getAttributes();
        final Set<String> notExpectedAttributes = new HashSet<>(inputAttributes.keySet());
        notExpectedAttributes.removeAll(expectedAttributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(FilterAttributes.REL_SUCCESS, 1);
        final MockFlowFile result = runner.getFlowFilesForRelationship(FilterAttributes.REL_SUCCESS).getFirst();
        result.assertContentEquals(exampleContent);
        for (String expectedName : expectedAttributes) {
            final String expectedValue = inputAttributes.get(expectedName);

            result.assertAttributeEquals(expectedName, expectedValue);
        }
        for (String notExpectedName : notExpectedAttributes) {
            result.assertAttributeNotExists(notExpectedName);
        }
    }
}