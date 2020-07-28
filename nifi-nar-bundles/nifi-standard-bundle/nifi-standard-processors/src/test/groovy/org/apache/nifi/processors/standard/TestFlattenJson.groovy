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
}
