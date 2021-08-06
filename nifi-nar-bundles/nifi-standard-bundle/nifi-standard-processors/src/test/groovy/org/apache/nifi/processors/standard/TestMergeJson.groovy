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
package org.apache.nifi.processors.standard


import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.Before
import org.junit.Test

import static groovy.json.JsonOutput.toJson

class TestMergeJson {
    static final ObjectMapper MAPPER = new ObjectMapper()
    TestRunner runner

    @Before
    void setup() {
        runner = TestRunners.newTestRunner(MergeJson.class)
        runner.setProperty(MergeJson.TIMEOUT, "10 sec")
        runner.setProperty(MergeJson.MAX_SIZE, "10 kb")
        runner.assertValid()
    }

    @Test
    void testDefaults() {
        0.upto(5000) {
            def map = [
                    msg: "Testing 1, 2, 3, 4, Testing... Is this mic on? Who's on first? Testing..."
            ]
            runner.enqueue(toJson(map))
        }
        def start = System.currentTimeMillis()
        runner.run()
        runner.assertTransferCount(MergeJson.REL_SUCCESS, 1)
        runner.assertTransferCount(MergeJson.REL_FAILURE, 0)
        def success = runner.getFlowFilesForRelationship(MergeJson.REL_SUCCESS)
        def raw = runner.getContentAsByteArray(success[0])
        assert MAPPER.readValue(raw, List.class)?.size() > 0
        assert runner.getFlowFilesForRelationship(MergeJson.REL_ORIGINAL)
        assert System.currentTimeMillis() < (start + 2000)
    }

    @Test
    void testMergeArrays() {
        def x = [
                [
                        msg: "X"
                ]
        ]
        def y = [
                [
                        msg: "Y"
                ]
        ]
        runner.enqueue(toJson(x))
        runner.enqueue(toJson(y))
        runner.run()
        runner.assertTransferCount(MergeJson.REL_SUCCESS, 1)
        runner.assertTransferCount(MergeJson.REL_FAILURE, 0)
        runner.assertTransferCount(MergeJson.REL_ORIGINAL, 2)

        def raw = runner.getContentAsByteArray(runner.getFlowFilesForRelationship(MergeJson.REL_SUCCESS)[0])
        def list = MAPPER.readValue(raw, List.class)
        assert list?.size() == 2
        assert list[0] instanceof Map
        assert list[0]["msg"] ==  "X"
        assert list[1] instanceof Map
        assert list[1]["msg"] == "Y"
    }

    @Test
    void testDontMergeArrays() {
        def x = [
                [
                        msg: "X"
                ]
        ]
        def y = [
                [
                        msg: "Y"
                ]
        ]
        runner.setProperty(MergeJson.MERGE_TOP_LEVEL_ARRAYS, "false")
        runner.enqueue(toJson(x))
        runner.enqueue(toJson(y))
        runner.run()
        runner.assertTransferCount(MergeJson.REL_SUCCESS, 1)
        runner.assertTransferCount(MergeJson.REL_FAILURE, 0)
        runner.assertTransferCount(MergeJson.REL_ORIGINAL, 2)

        def raw = runner.getContentAsByteArray(runner.getFlowFilesForRelationship(MergeJson.REL_SUCCESS)[0])
        def list = MAPPER.readValue(raw, List.class)
        assert list?.size() == 2
        assert list[0] instanceof List
        assert list[0][0] instanceof Map
        assert list[0][0]["msg"] ==  "X"
        assert list[1] instanceof List
        assert list[1][0] instanceof Map
        assert list[1][0]["msg"] ==  "Y"
    }

    @Test
    void testErrorHandling() {
        runner = TestRunners.newTestRunner(MergeJson.class)
        runner.setProperty(MergeJson.TIMEOUT, "100ms")
        runner.setProperty(MergeJson.MAX_SIZE, "10 kb")
        runner.enqueue("{x: y}")
        runner.run()
        runner.assertAllFlowFilesTransferred(MergeJson.REL_FAILURE)
    }
}
