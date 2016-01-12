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
package org.apache.nifi.processors;

import org.junit.Assert;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class WriteResourceToStreamTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(WriteResourceToStream.class);
    }

    @Test
    public void testProcessor() {
        testRunner.enqueue(new byte[] { 1, 2, 3, 4, 5 });
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(WriteResourceToStream.REL_SUCCESS, 1);
        final byte[] data = testRunner
                .getFlowFilesForRelationship(WriteResourceToStream.REL_SUCCESS).get(0)
                .toByteArray();
        final String stringData = new String(data);
        Assert.assertEquals("this came from a resource", stringData);
    }

}
