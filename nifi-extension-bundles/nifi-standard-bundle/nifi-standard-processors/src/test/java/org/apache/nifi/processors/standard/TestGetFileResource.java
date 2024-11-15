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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

/**
 * Unit tests for the GetFileResource processor.
 */
public class TestGetFileResource {

    @Test
    public void testInvalidConfiguration() {
        final TestRunner runner = TestRunners.newTestRunner(new GetFileResource());
        runner.setProperty(GetFileResource.FILE_RESOURCE, "");
        runner.assertNotValid();
    }

    @Test
    public void testGetFileResource() throws IOException {
        final String filePath = "src/test/resources/TestCountText/jabberwocky.txt";
        final TestRunner runner = TestRunners.newTestRunner(new GetFileResource());

        runner.setProperty(GetFileResource.FILE_RESOURCE, filePath);
        runner.setProperty(GetFileResource.MIME_TYPE, "text/plain");
        runner.setProperty("foo", "foo");
        runner.assertValid();

        runner.run();

        runner.assertTransferCount(GenerateFlowFile.SUCCESS, 1);
        final MockFlowFile ff = runner.getFlowFilesForRelationship(GenerateFlowFile.SUCCESS).get(0);
        ff.assertContentEquals(new File(filePath));
        ff.assertAttributeEquals("foo", "foo");
        ff.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain");
    }

}