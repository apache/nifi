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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestFilterAttribute {

    private final TestRunner runner = TestRunners.newTestRunner(FilterAttribute.class);

    private final String exampleContent = "lorem ipsum dolor sit amet";

    private final Map<String, String> exampleAttributes = Map.of(
            "foo", "fooValue",
            "bar", "barValue",
            "batz", "batzValue"
    );

    @Nested
    class WithStrategyEnumeration {
        @Nested
        class InModeRetain {
            @Test
            void retainsAllAttributesWhenAllAreFiltered() {
                final String attributeSet = "foo,bar,batz";
                final Set<String> expectedAttributes = Set.of("foo", "bar", "batz", "uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void retainsUUIDAndFilteredAttributesWhenOnlySomeAreFiltered() {
                final String attributeSet = "bar";
                final Set<String> expectedAttributes = Set.of("bar", "uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void retainsUUIDOnlyWhenNoneOfTheAttributesAreFiltered() {
                final String attributeSet = "other";
                final Set<String> expectedAttributes = Set.of("uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void supportsAttributeNamesWithWhitespace() {
                final Map<String, String> attributes = new HashMap<>(exampleAttributes);
                attributes.put("fo\no", "some value");
                final String attributeSet = "fo\no";
                final Set<String> expectedAttributes = Set.of("fo\no", "uuid");

                runTestWith(attributes, attributeSet, expectedAttributes);
            }
        }

        @Nested
        class InModeRemove {

            @BeforeEach
            void setUp() {
                runner.setProperty(FilterAttribute.FILTER_MODE, FilterAttribute.FilterMode.REMOVE);
            }

            @Test
            void removesAllAttributesExceptUUIDWhenAllAreFiltered() {
                final String attributeSet = "foo,bar,batz,uuid,path,filename";
                final Set<String> expectedAttributes = Set.of("uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void removesFilteredAttributesExceptUUIDWhenOnlySomeAreFiltered() {
                final String attributeSet = "bar,uuid,path,filename";
                final Set<String> expectedAttributes = Set.of("foo", "batz", "uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void removesNoAttributeWhenNoneOfTheAttributesAreFiltered() {
                final String attributeSet = "other";
                final Set<String> expectedAttributes = Set.of("foo", "bar", "batz", "uuid", "path", "filename");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void supportsAttributeNamesWithWhitespace() {
                final Map<String, String> attributes = new HashMap<>(exampleAttributes);
                attributes.put("fo\no", "some value");
                final String attributeSet = "fo\no";
                final Set<String> expectedAttributes = Set.of("foo", "bar", "batz", "uuid", "path", "filename");

                runTestWith(attributes, attributeSet, expectedAttributes);
            }
        }

        @Nested
        class RegardingAttributeSetParsing {

            @Test
            void ignoresLeadingDelimiters() {
                final String attributeSet = ",foo,bar";
                final Set<String> expectedAttributes = Set.of("foo", "bar", "uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void ignoresTrailingDelimiters() {
                final String attributeSet = "foo,bar,";
                final Set<String> expectedAttributes = Set.of("foo", "bar", "uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void doesNotYieldErrorWhenAttributeSetIsEffectivelyEmpty() {
                final String attributeSet = " , ";
                final Set<String> expectedAttributes = Set.of("uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void worksWithSingleAttributeInSet() {
                final String attributeSet = "batz";
                final Set<String> expectedAttributes = Set.of("batz", "uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void worksWithMultipleAttributesInSet() {
                final String attributeSet = "foo,bar,batz";
                final Set<String> expectedAttributes = Set.of("foo", "bar", "batz", "uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void ignoresLeadingWhitespaceInAttributeName() {
                final String attributeSet = "foo,  batz";
                final Set<String> expectedAttributes = Set.of("foo", "batz", "uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }

            @Test
            void ignoresTrailingWhitespaceInAttributeName() {
                final String attributeSet = "foo  ,bar";
                final Set<String> expectedAttributes = Set.of("foo", "bar", "uuid");

                runTestWith(exampleAttributes, attributeSet, expectedAttributes);
            }
        }

        @Test
        void supportsDefiningAttributeSetInFlowFileAttribute() {
            final Map<String, String> attributes = new HashMap<>(exampleAttributes);
            attributes.put("lookup", "bar,batz");
            final String attributeSet = "${lookup}"; // NiFi EL with reference to FlowFile attribute
            final Set<String> expectedAttributes = Set.of("bar", "batz", "uuid");

            runTestWith(attributes, attributeSet, expectedAttributes);
        }

        private void runTestWith(Map<String, String> attributes, String attributeSet, Set<String> expectedAttributes) {
            runner.setProperty(FilterAttribute.MATCHING_STRATEGY, FilterAttribute.MatchingStrategy.ENUMERATION);
            runner.setProperty(FilterAttribute.ATTRIBUTE_ENUMERATION, attributeSet);

            final MockFlowFile input = runner.enqueue(exampleContent, attributes);
            final Map<String, String> inputAttributes = input.getAttributes();
            final Set<String> notExpectedAttributes = new HashSet<>(inputAttributes.keySet());
            notExpectedAttributes.removeAll(expectedAttributes);

            runner.run();

            runner.assertAllFlowFilesTransferred(FilterAttribute.REL_SUCCESS, 1);
            final MockFlowFile result = runner.getFlowFilesForRelationship(FilterAttribute.REL_SUCCESS).getFirst();
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

    @Nested
    class WithStrategyRegex {

        @Nested
        class InModeRetain {
            @Test
            void retainsAllAttributesWhenAllAreFiltered() {
                final Pattern attributeRegex = Pattern.compile("foo|bar|batz");
                final Set<String> expectedAttributes = Set.of("foo", "bar", "batz", "uuid");

                runTestWith(exampleAttributes, attributeRegex, expectedAttributes);
            }

            @Test
            void retainsUUIDAndFilteredAttributesWhenOnlySomeAreFiltered() {
                final Pattern attributeRegex = Pattern.compile("bar");
                final Set<String> expectedAttributes = Set.of("bar", "uuid");

                runTestWith(exampleAttributes, attributeRegex, expectedAttributes);
            }

            @Test
            void retainsUUIDOnlyWhenNoneOfTheAttributesAreFiltered() {
                final Pattern attributeRegex = Pattern.compile("other");
                final Set<String> expectedAttributes = Set.of("uuid");

                runTestWith(exampleAttributes, attributeRegex, expectedAttributes);
            }

            @Test
            void supportsAttributeNamesWithWhitespace() {
                final Map<String, String> attributes = new HashMap<>(exampleAttributes);
                attributes.put("fo\no", "some value");
                final Pattern attributeRegex = Pattern.compile("fo\no");
                final Set<String> expectedAttributes = Set.of("fo\no", "uuid");

                runTestWith(attributes, attributeRegex, expectedAttributes);
            }
        }

        @Nested
        class InModeRemove {

            @BeforeEach
            void setUp() {
                runner.setProperty(FilterAttribute.FILTER_MODE, FilterAttribute.FilterMode.REMOVE);
            }

            @Test
            void removesAllAttributesExceptUUIDWhenAllAreFiltered() {
                final Pattern attributeRegex = Pattern.compile("foo|bar|batz|uuid|path|filename");
                final Set<String> expectedAttributes = Set.of("uuid");

                runTestWith(exampleAttributes, attributeRegex, expectedAttributes);
            }

            @Test
            void removesFilteredAttributesExceptUUIDWhenOnlySomeAreFiltered() {
                final Pattern attributeRegex = Pattern.compile("bar|uuid|path|filename");
                final Set<String> expectedAttributes = Set.of("foo", "batz", "uuid");

                runTestWith(exampleAttributes, attributeRegex, expectedAttributes);
            }

            @Test
            void removesNoAttributeWhenNoneOfTheAttributesAreFiltered() {
                final Pattern attributeRegex = Pattern.compile("other");
                final Set<String> expectedAttributes = Set.of("foo", "bar", "batz", "uuid", "path", "filename");

                runTestWith(exampleAttributes, attributeRegex, expectedAttributes);
            }

            @Test
            void supportsAttributeNamesWithWhitespace() {
                final Map<String, String> attributes = new HashMap<>(exampleAttributes);
                attributes.put("fo\no", "some value");
                final Pattern attributeRegex = Pattern.compile("fo\no");
                final Set<String> expectedAttributes = Set.of("foo", "bar", "batz", "uuid", "path", "filename");

                runTestWith(attributes, attributeRegex, expectedAttributes);
            }
        }

        @Test
        void supportsDefiningAttributeSetInFlowFileAttribute() {
            final Map<String, String> attributes = new HashMap<>(exampleAttributes);
            attributes.put("lookup", "bar|batz");
            final String attributeRegex = "${lookup}"; // NiFi EL with reference to FlowFile attribute
            final Set<String> expectedAttributes = Set.of("bar", "batz", "uuid");

            runTestWith(attributes, attributeRegex, expectedAttributes);
        }

        private void runTestWith(Map<String, String> attributes, Pattern regex, Set<String> expectedAttributes) {
            runTestWith(attributes, regex.pattern(), expectedAttributes);
        }

        private void runTestWith(Map<String, String> attributes, String regexPattern, Set<String> expectedAttributes) {
            runner.setProperty(FilterAttribute.MATCHING_STRATEGY, FilterAttribute.MatchingStrategy.PATTERN);
            runner.setProperty(FilterAttribute.ATTRIBUTE_PATTERN, regexPattern);

            final MockFlowFile input = runner.enqueue(exampleContent, attributes);
            final Map<String, String> inputAttributes = input.getAttributes();
            final Set<String> notExpectedAttributes = new HashSet<>(inputAttributes.keySet());
            notExpectedAttributes.removeAll(expectedAttributes);

            runner.run();

            runner.assertAllFlowFilesTransferred(FilterAttribute.REL_SUCCESS, 1);
            final MockFlowFile result = runner.getFlowFilesForRelationship(FilterAttribute.REL_SUCCESS).getFirst();
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
        runner.setProperty(FilterAttribute.ATTRIBUTE_ENUMERATION, "foo");

        runner.run(flowFileCount);
        runner.assertAllFlowFilesTransferred(FilterAttribute.REL_SUCCESS, flowFileCount);
        List<MockFlowFile> resultFlowFiles = runner.getFlowFilesForRelationship(FilterAttribute.REL_SUCCESS);
        for (final MockFlowFile resultFlowFile : resultFlowFiles) {
            resultFlowFile.assertAttributeExists("foo");
            resultFlowFile.assertAttributeNotExists("bar");
        }
        final Set<String> fooValues = resultFlowFiles.stream()
                .map(flowFile -> flowFile.getAttribute("foo"))
                .collect(Collectors.toUnmodifiableSet());
        assertEquals(flowFileCount, fooValues.size());
    }
}