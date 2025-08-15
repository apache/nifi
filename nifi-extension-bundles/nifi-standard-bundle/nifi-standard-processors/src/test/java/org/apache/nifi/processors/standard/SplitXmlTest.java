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

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SplitXmlTest {

    private static final String DOCTYPE_DOCUMENT = """
            <!DOCTYPE ANY SYSTEM "http://localhost">
            """;

    private TestRunner runner;

    @BeforeEach
    void setupRunner() {
        runner = TestRunners.newTestRunner(new SplitXml());
    }

    @Test
    void testDocumentTypeDisallowed() {
        runner.enqueue(DOCTYPE_DOCUMENT);

        runner.run();

        runner.assertAllFlowFilesTransferred(SplitXml.REL_FAILURE);
    }
}
