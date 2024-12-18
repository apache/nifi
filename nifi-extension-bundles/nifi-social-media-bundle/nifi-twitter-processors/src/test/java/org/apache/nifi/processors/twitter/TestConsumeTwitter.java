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
package org.apache.nifi.processors.twitter;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;

public class TestConsumeTwitter {
    private MockWebServer mockWebServer;

    private TestRunner runner;

    @BeforeEach
    public void setRunnerAndAPI() {
        mockWebServer = new MockWebServer();

        runner = TestRunners.newTestRunner(ConsumeTwitter.class);

        runner.setProperty(ConsumeTwitter.BEARER_TOKEN, "BEARER_TOKEN");
        final String basePath = mockWebServer.url("").toString();
        runner.setProperty(ConsumeTwitter.BASE_PATH, basePath);
    }

    @AfterEach
    public void shutdownServerAndAPI() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    @Timeout(60)
    public void testReceiveSingleTweetInStream() {
        String sampleTweet = "{\"data\":{\"id\":\"123\",\"text\":\"This is a sample tweet and is not real!\"}}";
        MockResponse response = new MockResponse()
                .setResponseCode(200)
                .setBody(sampleTweet)
                .addHeader("Content-Type", "application/json");
        mockWebServer.enqueue(response);


        runner.setProperty(ConsumeTwitter.ENDPOINT, ConsumeTwitter.ENDPOINT_SAMPLE);
        runner.setProperty(ConsumeTwitter.QUEUE_SIZE, "10000");
        runner.setProperty(ConsumeTwitter.BATCH_SIZE, "10");
        runner.setProperty(ConsumeTwitter.BACKFILL_MINUTES, "0");

        runner.assertValid();

        // The TwitterStreamAPI class spins up another thread and might not be done queueing tweets in one run of the
        // processor, so the test will timeout after 60 seconds.
        runner.run(1, false, true);

        while (runner.getFlowFilesForRelationship(ConsumeTwitter.REL_SUCCESS).isEmpty()) {
            runner.run(1, false, false);
        }
        runner.stop();

        // there should only be a single FlowFile containing a tweet
        runner.assertTransferCount(ConsumeTwitter.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConsumeTwitter.REL_SUCCESS).getFirst();
        String expectedTweet = "[{\"data\":{\"id\":\"123\",\"text\":\"This is a sample tweet and is not real!\"}}]";
        flowFile.assertContentEquals(expectedTweet);
        flowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        flowFile.assertAttributeEquals("tweets", "1");
    }
}
