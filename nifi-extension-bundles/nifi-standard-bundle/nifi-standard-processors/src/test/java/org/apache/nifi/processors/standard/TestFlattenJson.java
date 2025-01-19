/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class TestFlattenJson {
    private static final ObjectMapper mapper = new ObjectMapper();

    private TestRunner testRunner;

    @BeforeEach
    void setupRunner() {
        testRunner = TestRunners.newTestRunner(FlattenJson.class);
    }

    @Test
    void testFlatten() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"test\": {\n" +
                "        \"msg\": \"Hello, world\"\n" +
                "    },\n" +
                "    \"first\": {\n" +
                "        \"second\": {\n" +
                "            \"third\": [\n" +
                "                \"one\",\n" +
                "                \"two\",\n" +
                "                \"three\",\n" +
                "                \"four\",\n" +
                "                \"five\"\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        final Map parsed = (Map) baseTest(testRunner, json, 2);
        assertEquals(parsed.get("test.msg"), "Hello, world", "test.msg should exist, but doesn't");
        assertEquals(parsed.get("first.second.third"),
                Arrays.asList("one", "two", "three", "four", "five"),
                "Three level block doesn't exist.");
    }

    @Test
    void testFlattenRecordSet() throws JsonProcessingException {
        final String json = "[\n" +
                "    {\n" +
                "        \"first\": {\n" +
                "            \"second\": \"Hello\"\n" +
                "        }\n" +
                "    },\n" +
                "    {\n" +
                "        \"first\": {\n" +
                "            \"second\": \"World\"\n" +
                "        }\n" +
                "    }\n" +
                "]";

        final List<String> expected = Arrays.asList("Hello", "World");
        final List parsed = (List) baseTest(testRunner, json, 2);
        assertInstanceOf(List.class, parsed, "Not a list");
        for (int i = 0; i < parsed.size(); i++) {
            final Map map = (Map) parsed.get(i);
            assertEquals(map.get("first.second"), expected.get(i), "Missing values.");
        }
    }

    @Test
    void testDifferentSeparator() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first\": {\n" +
                "        \"second\": {\n" +
                "            \"third\": [\n" +
                "                \"one\",\n" +
                "                \"two\",\n" +
                "                \"three\",\n" +
                "                \"four\",\n" +
                "                \"five\"\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";
        testRunner.setProperty(FlattenJson.SEPARATOR, "_");
        final Map parsed = (Map) baseTest(testRunner, json, 1);

        assertEquals(parsed.get("first_second_third"),
                Arrays.asList("one", "two", "three", "four", "five"),
                "Separator not applied.");
    }

    @Test
    void testExpressionLanguage() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first\": {\n" +
                "        \"second\": {\n" +
                "            \"third\": [\n" +
                "                \"one\",\n" +
                "                \"two\",\n" +
                "                \"three\",\n" +
                "                \"four\",\n" +
                "                \"five\"\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";

        testRunner.setValidateExpressionUsage(true);
        testRunner.setProperty(FlattenJson.SEPARATOR, "${separator.char}");
        final Map parsed = (Map) baseTest(testRunner, json, Collections.singletonMap("separator.char", "_"), 1);
        assertEquals(parsed.get("first_second_third"),
                Arrays.asList("one", "two", "three", "four", "five"),
                "Separator not applied.");
    }

    @Test
    void testFlattenModeNormal() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first\": {\n" +
                "        \"second\": {\n" +
                "            \"third\": [\n" +
                "                \"one\",\n" +
                "                \"two\",\n" +
                "                \"three\",\n" +
                "                \"four\",\n" +
                "                \"five\"\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_NORMAL);
        final Map parsed = (Map) baseTest(testRunner, json, 5);
        assertEquals("one", parsed.get("first.second.third[0]"), "Separator not applied.");
    }

    @Test
    void testFlattenModeKeepArrays() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first\": {\n" +
                "        \"second\": [\n" +
                "            {\n" +
                "                \"x\": 1,\n" +
                "                \"y\": 2,\n" +
                "                \"z\": [\n" +
                "                    3,\n" +
                "                    4,\n" +
                "                    5\n" +
                "                ]\n" +
                "            },\n" +
                "            [\n" +
                "                6,\n" +
                "                7,\n" +
                "                8\n" +
                "            ],\n" +
                "            [\n" +
                "                [\n" +
                "                    9,\n" +
                "                    10\n" +
                "                ],\n" +
                "                11,\n" +
                "                12\n" +
                "            ]\n" +
                "        ],\n" +
                "        \"third\": {\n" +
                "            \"a\": \"b\",\n" +
                "            \"c\": \"d\",\n" +
                "            \"e\": \"f\"\n" +
                "        }\n" +
                "    }\n" +
                "}";

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_KEEP_ARRAYS);
        final Map parsed = (Map) baseTest(testRunner, json, 4);
        assertInstanceOf(List.class, parsed.get("first.second"));
        assertEquals(Arrays.asList(6, 7, 8), ((List) parsed.get("first.second")).get(1));
        assertEquals("b", parsed.get("first.third.a"), "Separator not applied.");
    }

    @Test
    void testFlattenModeKeepPrimitiveArrays() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first\": {\n" +
                "        \"second\": [\n" +
                "            {\n" +
                "                \"x\": 1,\n" +
                "                \"y\": 2,\n" +
                "                \"z\": [\n" +
                "                    3,\n" +
                "                    4,\n" +
                "                    5\n" +
                "                ]\n" +
                "            },\n" +
                "            [\n" +
                "                6,\n" +
                "                7,\n" +
                "                8\n" +
                "            ],\n" +
                "            [\n" +
                "                [\n" +
                "                    9,\n" +
                "                    10\n" +
                "                ],\n" +
                "                11,\n" +
                "                12\n" +
                "            ]\n" +
                "        ],\n" +
                "        \"third\": {\n" +
                "            \"a\": \"b\",\n" +
                "            \"c\": \"d\",\n" +
                "            \"e\": \"f\"\n" +
                "        }\n" +
                "    }\n" +
                "}";

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_KEEP_PRIMITIVE_ARRAYS);
        final Map parsed = (Map) baseTest(testRunner, json, 10);
        assertEquals(1, parsed.get("first.second[0].x"), "Separator not applied.");
        assertEquals(Arrays.asList(3, 4, 5), parsed.get("first.second[0].z"), "Separator not applied.");
        assertEquals(Arrays.asList(9, 10), parsed.get("first.second[2][0]"), "Separator not applied.");
        assertEquals(11, parsed.get("first.second[2][1]"), "Separator not applied.");
        assertEquals(12, parsed.get("first.second[2][2]"), "Separator not applied.");
        assertEquals("d", parsed.get("first.third.c"), "Separator not applied.");
    }

    @Test
    void testFlattenModeDotNotation() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first\": {\n" +
                "        \"second\": {\n" +
                "            \"third\": [\n" +
                "                \"one\",\n" +
                "                \"two\",\n" +
                "                \"three\",\n" +
                "                \"four\",\n" +
                "                \"five\"\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_DOT_NOTATION);
        final Map parsed = (Map) baseTest(testRunner, json, 5);
        assertEquals("one", parsed.get("first.second.third.0"), "Separator not applied.");
    }

    @Test
    void testFlattenSlash() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first\": {\n" +
                "        \"second\": {\n" +
                "            \"third\": [\n" +
                "                \"http://localhost/value1\",\n" +
                "                \"http://localhost/value2\"\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}";

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_NORMAL);
        final Map parsed = (Map) baseTest(testRunner, json, 2);
        assertEquals("http://localhost/value1", parsed.get("first.second.third[0]"), "Separator not applied.");
    }

    @Test
    void testEscapeForJson() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"name\": \"Jos\\u00e9\"\n" +
                "}";

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_NORMAL);
        final Map parsed = (Map) baseTest(testRunner, json, 1);
        assertEquals("José", parsed.get("name"), "Separator not applied.");
    }

    @Test
    void testUnFlatten() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"test.msg\": \"Hello, world\",\n" +
                "    \"first.second.third\": [\n" +
                "        \"one\",\n" +
                "        \"two\",\n" +
                "        \"three\",\n" +
                "        \"four\",\n" +
                "        \"five\"\n" +
                "    ]\n" +
                "}";

        testRunner.setProperty(FlattenJson.RETURN_TYPE, FlattenJson.RETURN_TYPE_UNFLATTEN);
        final Map parsed = (Map) baseTest(testRunner, json, 2);
        assertEquals("Hello, world", ((Map) parsed.get("test")).get("msg"));
        assertEquals(Arrays.asList("one", "two", "three", "four", "five"),
                ((Map) ((Map) parsed.get("first")).get("second")).get("third"));
    }

    @Test
    void testUnFlattenWithDifferentSeparator() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first_second_third\": [\n" +
                "        \"one\",\n" +
                "        \"two\",\n" +
                "        \"three\",\n" +
                "        \"four\",\n" +
                "        \"five\"\n" +
                "    ]\n" +
                "}";

        testRunner.setProperty(FlattenJson.SEPARATOR, "_");
        testRunner.setProperty(FlattenJson.RETURN_TYPE, FlattenJson.RETURN_TYPE_UNFLATTEN);
        final Map parsed = (Map) baseTest(testRunner, json, 1);
        assertEquals(Arrays.asList("one", "two", "three", "four", "five"),
                ((Map) ((Map) parsed.get("first")).get("second")).get("third"));
    }

    @Test
    void testUnFlattenForKeepArraysMode() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"a.b\": 1,\n" +
                "    \"a.c\": [\n" +
                "        false,\n" +
                "        {\n" +
                "            \"i.j\": [\n" +
                "                false,\n" +
                "                true,\n" +
                "                \"xy\"\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_KEEP_ARRAYS);
        testRunner.setProperty(FlattenJson.RETURN_TYPE, FlattenJson.RETURN_TYPE_UNFLATTEN);
        final Map parsed = (Map) baseTest(testRunner, json, 1);
        assertEquals(1, ((Map) parsed.get("a")).get("b"));
        assertEquals(false, ((List) ((Map) parsed.get("a")).get("c")).get(0));
        assertEquals(Arrays.asList(false, true, "xy"),
                ((Map) ((Map) ((List) ((Map) parsed.get("a")).get("c")).get(1)).get("i")).get("j"));
    }

    @Test
    void testUnFlattenForKeepPrimitiveArraysMode() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first.second[0].x\": 1,\n" +
                "    \"first.second[0].y\": 2,\n" +
                "    \"first.second[0].z\": [\n" +
                "        3,\n" +
                "        4,\n" +
                "        5\n" +
                "    ],\n" +
                "    \"first.second[1]\": [\n" +
                "        6,\n" +
                "        7,\n" +
                "        8\n" +
                "    ],\n" +
                "    \"first.second[2][0]\": [\n" +
                "        9,\n" +
                "        10\n" +
                "    ],\n" +
                "    \"first.second[2][1]\": 11,\n" +
                "    \"first.second[2][2]\": 12,\n" +
                "    \"first.third.a\": \"b\",\n" +
                "    \"first.third.c\": \"d\",\n" +
                "    \"first.third.e\": \"f\"\n" +
                "}";

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_KEEP_PRIMITIVE_ARRAYS);
        testRunner.setProperty(FlattenJson.RETURN_TYPE, FlattenJson.RETURN_TYPE_UNFLATTEN);
        final Map parsed = (Map) baseTest(testRunner, json, 1);
        assertEquals(1, ((Map) ((List) ((Map) parsed.get("first")).get("second")).get(0)).get("x"));
        assertEquals(Arrays.asList(9, 10), ((List) ((List) ((Map) parsed.get("first")).get("second")).get(2)).get(0));
        assertEquals("d", ((Map) ((Map) parsed.get("first")).get("third")).get("c"));
    }

    @Test
    void testUnFlattenForDotNotationMode() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first.second.third.0\": [\n" +
                "        \"one\",\n" +
                "        \"two\",\n" +
                "        \"three\",\n" +
                "        \"four\",\n" +
                "        \"five\"\n" +
                "    ]\n" +
                "}";

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_DOT_NOTATION);
        testRunner.setProperty(FlattenJson.RETURN_TYPE, FlattenJson.RETURN_TYPE_UNFLATTEN);

        final Map parsed = (Map) baseTest(testRunner, json, 1);
        assertEquals(Arrays.asList("one", "two", "three", "four", "five"),
                ((List) ((Map) ((Map) parsed.get("first")).get("second")).get("third")).get(0));
    }

    @Test
    void testFlattenWithIgnoreReservedCharacters() throws JsonProcessingException {
        final String json = "{\n" +
                "    \"first\": {\n" +
                "        \"second.third\": \"Hello\",\n" +
                "        \"fourth\": \"World\"\n" +
                "    }\n" +
                "}";

        testRunner.setProperty(FlattenJson.IGNORE_RESERVED_CHARACTERS, "true");

        final Map parsed = (Map) baseTest(testRunner, json, 2);
        assertEquals("Hello", parsed.get("first.second.third"), "Separator not applied.");
        assertEquals("World", parsed.get("first.fourth"), "Separator not applied.");
    }

    @Test
    void testFlattenRecordSetWithIgnoreReservedCharacters() throws JsonProcessingException {
        final String json = "[\n" +
                "    {\n" +
                "        \"first\": {\n" +
                "            \"second_third\": \"Hello\"\n" +
                "        }\n" +
                "    },\n" +
                "    {\n" +
                "        \"first\": {\n" +
                "            \"second_third\": \"World\"\n" +
                "        }\n" +
                "    }\n" +
                "]";
        testRunner.setProperty(FlattenJson.SEPARATOR, "_");
        testRunner.setProperty(FlattenJson.IGNORE_RESERVED_CHARACTERS, "true");

        final List<String> expected = Arrays.asList("Hello", "World");

        final List parsed = (List) baseTest(testRunner, json, 2);
        for (int i = 0; i < parsed.size(); i++) {
            assertEquals(expected.get(i), ((Map) parsed.get(i)).get("first_second_third"), "Missing values.");
        }
    }

    @Test
    void testFlattenModeNormalWithIgnoreReservedCharacters() throws JsonProcessingException {
        final String json = "[\n" +
                "    {\n" +
                "        \"first\": {\n" +
                "            \"second_third\": \"Hello\"\n" +
                "        }\n" +
                "    },\n" +
                "    {\n" +
                "        \"first\": {\n" +
                "            \"second_third\": \"World\"\n" +
                "        }\n" +
                "    }\n" +
                "]";
        testRunner.setProperty(FlattenJson.SEPARATOR, "_");
        testRunner.setProperty(FlattenJson.IGNORE_RESERVED_CHARACTERS, "true");
        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_NORMAL);

        final Map parsed = (Map) baseTest(testRunner, json, 2);
        assertEquals("Hello", parsed.get("[0]_first_second_third"), "Separator not applied.");
        assertEquals("World", parsed.get("[1]_first_second_third"), "Separator not applied.");
    }

    private Object baseTest(TestRunner testRunner, String json, int keyCount) throws JsonProcessingException {
        return baseTest(testRunner, json, Collections.emptyMap(), keyCount);
    }

    private Object baseTest(TestRunner testRunner, String json, Map attrs, int keyCount) throws JsonProcessingException {
        testRunner.enqueue(json, attrs);
        testRunner.run(1, true);
        testRunner.assertTransferCount(FlattenJson.REL_FAILURE, 0);
        testRunner.assertTransferCount(FlattenJson.REL_SUCCESS, 1);

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(FlattenJson.REL_SUCCESS);
        final byte[] content = testRunner.getContentAsByteArray(flowFiles.get(0));
        final String asJson = new String(content);
        if (asJson.startsWith("[")) {
            final List parsed;
            parsed = mapper.readValue(asJson, List.class);
            assertEquals(keyCount, parsed.size(), "Too many keys");
            return parsed;
        } else {
            final Map parsed;
            parsed = mapper.readValue(asJson, Map.class);
            assertEquals(keyCount, parsed.size(), "Too many keys");
            return parsed;
        }
    }
}
