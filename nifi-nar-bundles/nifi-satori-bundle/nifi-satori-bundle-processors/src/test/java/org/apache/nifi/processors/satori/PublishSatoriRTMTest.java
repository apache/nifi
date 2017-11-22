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
package org.apache.nifi.processors.satori;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class PublishSatoriRTMTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PublishSatoriRTM.class);
    }

    @Test
    public void testProcessor() {

        // Content to be mock a geo csv file
        String inputJson = "{\"test1\":1,\"test2\":\"two\"}\n{test2}";
        InputStream content = new ByteArrayInputStream(inputJson.getBytes());

        // Generate a test runner to mock a processor in a flow
        TestRunner runner = TestRunners.newTestRunner(new PublishSatoriRTM());

        // Define satori connection properties
        runner.setProperty(PublishSatoriRTM.ENDPOINT, "wss://qwxhvwv6.api.satori.com");
        runner.setProperty(PublishSatoriRTM.APPKEY, "aaa");
        runner.setProperty(PublishSatoriRTM.CHANNEL, "nifitest");
        runner.setProperty(PublishSatoriRTM.MSG_DEMARCATOR, "\n");
        runner.assertValid();

        // Uncomment the lines below if you want to publish messages within this unit test.
        // Note, you will need to update the properties above to a relevant channel.

        //runner.enqueue(content);
        //runner.run(1);
    }

}
