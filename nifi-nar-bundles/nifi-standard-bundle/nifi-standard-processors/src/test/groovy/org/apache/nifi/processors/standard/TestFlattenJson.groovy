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

package org.apache.nifi.processors.standard

import groovy.json.JsonSlurper
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Test
import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class TestFlattenJson {
    @Test
    void testFlatten() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
            test: [
                msg: "Hello, world"
            ],
            first: [
                second: [
                    third: [
                        "one", "two", "three", "four", "five"
                    ]
                ]
            ]
        ]))
        baseTest(testRunner, json, 2) { parsed ->
            Assert.assertEquals("test.msg should exist, but doesn't", parsed["test.msg"], "Hello, world")
            Assert.assertEquals("Three level block doesn't exist.", parsed["first.second.third"], [
                    "one", "two", "three", "four", "five"
            ])
        }
    }

    void baseTest(testRunner, String json, int keyCount, Closure c) {
        baseTest(testRunner, json, [:], keyCount, c);
    }

    void baseTest(def testRunner, String json, Map attrs, int keyCount, Closure c) {
        testRunner.enqueue(json, attrs)
        testRunner.run(1, true)
        testRunner.assertTransferCount(FlattenJson.REL_FAILURE, 0)
        testRunner.assertTransferCount(FlattenJson.REL_SUCCESS, 1)

        def flowFiles = testRunner.getFlowFilesForRelationship(FlattenJson.REL_SUCCESS)
        def content   = testRunner.getContentAsByteArray(flowFiles[0])
        def asJson    = new String(content)
        def slurper   = new JsonSlurper()
        def parsed    = slurper.parseText(asJson) as Map

        Assert.assertEquals("Too many keys", keyCount, parsed.size())
        c.call(parsed)
    }

    @Test
    void testFlattenRecordSet() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
            [
                first: [
                    second: "Hello"
                ]
            ],
            [
                first: [
                    second: "World"
                ]
            ]
        ]))

        def expected = ["Hello", "World"]
        baseTest(testRunner, json, 2) { parsed ->
            Assert.assertTrue("Not a list", parsed instanceof List)
            0.upto(parsed.size() - 1) {
                Assert.assertEquals("Missing values.", parsed[it]["first.second"], expected[it])
            }
        }
    }

    @Test
    void testDifferentSeparator() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
            first: [
                second: [
                    third: [
                        "one", "two", "three", "four", "five"
                    ]
                ]
            ]
        ]))
        testRunner.setProperty(FlattenJson.SEPARATOR, "_")
        baseTest(testRunner, json, 1) { parsed ->
            Assert.assertEquals("Separator not applied.", parsed["first_second_third"], [
                "one", "two", "three", "four", "five"
            ])
        }
    }

    @Test
    void testExpressionLanguage() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
            first: [
                second: [
                    third: [
                        "one", "two", "three", "four", "five"
                    ]
                ]
            ]
        ]))

        testRunner.setValidateExpressionUsage(true);
        testRunner.setProperty(FlattenJson.SEPARATOR, '${separator.char}')
        baseTest(testRunner, json, ["separator.char": "_"], 1) { parsed ->
            Assert.assertEquals("Separator not applied.", parsed["first_second_third"], [
                "one", "two", "three", "four", "five"
            ])
        }
    }

    @Test
    void testFlattenModeNormal() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
                first: [
                        second: [
                                third: [
                                        "one", "two", "three", "four", "five"
                                ]
                        ]
                ]
        ]))

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_NORMAL)
        baseTest(testRunner, json,5) { parsed ->
            Assert.assertEquals("Separator not applied.", "one", parsed["first.second.third[0]"])
        }
    }

    @Test
    void testFlattenModeKeepArrays() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
                first: [
                        second: [
                                [
                                        x: 1,
                                        y: 2,
                                        z: [3, 4, 5]
                                ],
                                [ 6, 7, 8],
                                [
                                        [9, 10],
                                        11,
                                        12
                                ]
                        ],
                        "third" : [
                                a: "b",
                                c: "d",
                                e: "f"
                        ]
                ]
        ]))

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_KEEP_ARRAYS)
        baseTest(testRunner, json,4) { parsed ->
            assert parsed["first.second"] instanceof List   // [{x=1, y=2, z=[3, 4, 5]}, [6, 7, 8], [[9, 10], 11, 12]]
            assert parsed["first.second"][1] == [6, 7, 8]
            Assert.assertEquals("Separator not applied.", "b", parsed["first.third.a"])
        }
    }

    @Test
    void testFlattenModeKeepPrimitiveArrays() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
                first: [
                        second: [
                                [
                                        x: 1,
                                        y: 2,
                                        z: [3, 4, 5]
                                ],
                                [ 6, 7, 8],
                                [
                                        [9, 10],
                                        11,
                                        12
                                ]
                        ],
                        "third" : [
                                a: "b",
                                c: "d",
                                e: "f"
                        ]
                ]
        ]))

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_KEEP_PRIMITIVE_ARRAYS)
        baseTest(testRunner, json,10) { parsed ->
            Assert.assertEquals("Separator not applied.", 1, parsed["first.second[0].x"])
            Assert.assertEquals("Separator not applied.", [3, 4, 5], parsed["first.second[0].z"])
            Assert.assertEquals("Separator not applied.", [9, 10], parsed["first.second[2][0]"])
            Assert.assertEquals("Separator not applied.", 11, parsed["first.second[2][1]"])
            Assert.assertEquals("Separator not applied.", 12, parsed["first.second[2][2]"])
            Assert.assertEquals("Separator not applied.", "d", parsed["first.third.c"])
        }
    }

    @Test
    void testFlattenModeDotNotation() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
                first: [
                        second: [
                                third: [
                                        "one", "two", "three", "four", "five"
                                ]
                        ]
                ]
        ]))

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_DOT_NOTATION)
        baseTest(testRunner, json,5) { parsed ->
            Assert.assertEquals("Separator not applied.", "one", parsed["first.second.third.0"])
        }
    }

    @Test
    void testFlattenSlash() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
                first: [
                        second: [
                                third: [
                                        "http://localhost/value1", "http://localhost/value2"
                                ]
                        ]
                ]
        ]))

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_NORMAL)
        baseTest(testRunner, json,2) { parsed ->
            Assert.assertEquals("Separator not applied.", "http://localhost/value1", parsed["first.second.third[0]"])
        }
    }

    @Test
    void testEscapeForJson() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([ name: "José"
        ]))

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_NORMAL)
        baseTest(testRunner, json,1) { parsed ->
            Assert.assertEquals("Separator not applied.", "José", parsed["name"])
        }
    }

    @Test
    void testUnFlatten() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
                "test.msg": "Hello, world",
                "first.second.third":  [ "one", "two", "three", "four", "five" ]
        ]))

        testRunner.setProperty(FlattenJson.RETURN_TYPE, FlattenJson.RETURN_TYPE_UNFLATTEN)
        baseTest(testRunner, json, 2) { parsed ->
            assert parsed.test instanceof Map
            assert parsed.test.msg == "Hello, world"
            assert parsed.first.second.third == [ "one", "two", "three", "four", "five" ]
        }
    }

    @Test
    void testUnFlattenWithDifferentSeparator() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
                "first_second_third":  [ "one", "two", "three", "four", "five" ]
        ]))

        testRunner.setProperty(FlattenJson.SEPARATOR, "_")
        testRunner.setProperty(FlattenJson.RETURN_TYPE, FlattenJson.RETURN_TYPE_UNFLATTEN)
        baseTest(testRunner, json, 1) { parsed ->
            assert parsed.first instanceof Map
            assert parsed.first.second.third == [ "one", "two", "three", "four", "five" ]
        }
    }

    @Test
    void testUnFlattenForKeepArraysMode() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
                "a.b": 1,
                "a.c": [
                        false,
                        ["i.j": [ false, true, "xy" ] ]
                ]
        ]))

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_KEEP_ARRAYS)
        testRunner.setProperty(FlattenJson.RETURN_TYPE, FlattenJson.RETURN_TYPE_UNFLATTEN)
        baseTest(testRunner, json, 1) { parsed ->
            assert parsed.a instanceof Map
            assert parsed.a.b == 1
            assert parsed.a.c[0] == false
            assert parsed.a.c[1].i instanceof Map
            assert parsed.a.c[1].i.j == [false, true, "xy"]
        }
    }

    @Test
    void testUnFlattenForKeepPrimitiveArraysMode() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
                "first.second[0].x": 1,
                "first.second[0].y": 2,
                "first.second[0].z": [3, 4, 5],
                "first.second[1]": [6, 7, 8],
                "first.second[2][0]": [9, 10],
                "first.second[2][1]": 11,
                "first.second[2][2]": 12,
                "first.third.a": "b",
                "first.third.c": "d",
                "first.third.e": "f"
        ]))

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_KEEP_PRIMITIVE_ARRAYS)
        testRunner.setProperty(FlattenJson.RETURN_TYPE, FlattenJson.RETURN_TYPE_UNFLATTEN)
        baseTest(testRunner, json, 1) { parsed ->
            assert parsed.first instanceof Map
            assert parsed.first.second[0].x == 1
            assert parsed.first.second[2][0] == [9, 10]
            assert parsed.first.third.c == "d"
        }
    }

    @Test
    void testUnFlattenForDotNotationMode() {
        def testRunner = TestRunners.newTestRunner(FlattenJson.class)
        def json = prettyPrint(toJson([
                "first.second.third.0": ["one", "two", "three", "four", "five"]
        ]))

        testRunner.setProperty(FlattenJson.FLATTEN_MODE, FlattenJson.FLATTEN_MODE_DOT_NOTATION)
        testRunner.setProperty(FlattenJson.RETURN_TYPE, FlattenJson.RETURN_TYPE_UNFLATTEN)
        baseTest(testRunner, json,1) { parsed ->
            assert parsed.first instanceof Map
            assert parsed.first.second.third[0] == ["one", "two", "three", "four", "five"]
        }
    }
}
