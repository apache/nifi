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

import org.apache.nifi.processors.standard.RouteOnContent;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

public class TestRouteOnContent {

    @Test
    public void testCloning() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteOnContent());
        runner.setProperty(RouteOnContent.MATCH_REQUIREMENT, RouteOnContent.MATCH_SUBSEQUENCE);
        runner.setProperty("hello", "Hello");
        runner.setProperty("world", "World");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));

        runner.run();
        runner.assertTransferCount("hello", 1);
        runner.assertTransferCount("world", 1);
        runner.assertTransferCount(RouteOnContent.REL_NO_MATCH, 0);
    }

    @Test
    public void testSubstituteAttributes() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteOnContent());
        runner.setProperty(RouteOnContent.MATCH_REQUIREMENT, RouteOnContent.MATCH_SUBSEQUENCE);
        runner.setProperty("attr", "Hel${highLow}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("highLow", "lo");
        runner.enqueue(Paths.get("src/test/resources/hello.txt"), attributes);

        runner.run();
        runner.assertAllFlowFilesTransferred("attr", 1);
    }

    @Test
    public void testBufferSize() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new RouteOnContent());
        runner.setProperty(RouteOnContent.MATCH_REQUIREMENT, RouteOnContent.MATCH_ALL);
        runner.setProperty(RouteOnContent.BUFFER_SIZE, "3 B");
        runner.setProperty("rel", "Hel");

        runner.enqueue(Paths.get("src/test/resources/hello.txt"));

        runner.run();
        runner.assertAllFlowFilesTransferred("rel", 1);
    }
}
