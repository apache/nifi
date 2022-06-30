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

import groovy.json.JsonSlurper
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.Test

class WriteTextTest {
    @Test
    void test() {
        def runner = TestRunners.newTestRunner(WriteText.class)
        runner.setProperty(WriteText.TEXT, """
            {
                "msg": "\${message}"
            }
        """)
        def attributes = [
            message: "Hello, world!"
        ]
        runner.enqueue("", attributes)
        runner.run()

        runner.assertTransferCount(WriteText.REL_FAILURE, 0)
        runner.assertTransferCount(WriteText.REL_SUCCESS, 1)

        def ff = runner.getFlowFilesForRelationship(WriteText.REL_SUCCESS)[0]
        def content = runner.getContentAsByteArray(ff)
        def str = new String(content)
        def parsed = new JsonSlurper().parseText(str)
        assert parsed
        assert parsed["msg"] == attributes["message"]
    }
}
