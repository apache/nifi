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

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestConsumeTwitter {
    private MockWebServer mockWebServer;

    private TestRunner runner;

    @BeforeEach
    public void setRunnerAndAPI() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        runner = TestRunners.newTestRunner(ConsumeTwitter.class);

        runner.setProperty(ConsumeTwitter.BEARER_TOKEN, "BEARER_TOKEN");
        final String basePath = mockWebServer.url("").toString();
        runner.setProperty(ConsumeTwitter.BASE_PATH, basePath);
    }

    @AfterEach
    public void shutdownServerAndAPI() throws IOException {
        mockWebServer.close();
    }

    @Test
    @Timeout(60)
    public void testReceiveSingleTweetInStream() {
        String sampleTweet = "{\"data\":{\"id\":\"123\",\"text\":\"This is a sample tweet and is not real!\"}}";
        MockResponse response = new MockResponse.Builder()
                .code(200)
                .body(sampleTweet)
                .addHeader("Content-Type", "application/json")
                .build();
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

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("stream-endpoint", ConsumeTwitter.ENDPOINT.getName()),
                Map.entry("base-path", ConsumeTwitter.BASE_PATH.getName()),
                Map.entry("bearer-token", ConsumeTwitter.BEARER_TOKEN.getName()),
                Map.entry("queue-size", ConsumeTwitter.QUEUE_SIZE.getName()),
                Map.entry("batch-size", ConsumeTwitter.BATCH_SIZE.getName()),
                Map.entry("backoff-attempts", ConsumeTwitter.BACKOFF_ATTEMPTS.getName()),
                Map.entry("backoff-time", ConsumeTwitter.BACKOFF_TIME.getName()),
                Map.entry("maximum-backoff-time", ConsumeTwitter.MAXIMUM_BACKOFF_TIME.getName()),
                Map.entry("connect-timeout", ConsumeTwitter.CONNECT_TIMEOUT.getName()),
                Map.entry("read-timeout", ConsumeTwitter.READ_TIMEOUT.getName()),
                Map.entry("backfill-minutes", ConsumeTwitter.BACKFILL_MINUTES.getName()),
                Map.entry("tweet-fields", ConsumeTwitter.TWEET_FIELDS.getName()),
                Map.entry("user-fields", ConsumeTwitter.USER_FIELDS.getName()),
                Map.entry("media-fields", ConsumeTwitter.MEDIA_FIELDS.getName()),
                Map.entry("poll-fields", ConsumeTwitter.POLL_FIELDS.getName()),
                Map.entry("place-fields", ConsumeTwitter.PLACE_FIELDS.getName()),
                Map.entry("expansions", ConsumeTwitter.EXPANSIONS.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }
}
