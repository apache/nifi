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

package org.apache.nifi.processors.slack;

import org.apache.nifi.processors.slack.ConsumeSlack.ConversationHistoryClient;
import org.apache.nifi.processors.slack.ConsumeSlack.HttpResponse;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestConsumeSlack {
    private TestRunner testRunner;
    private HttpResponse nextResponse;
    private final List<MockConversationHistoryClient> clients = new ArrayList<>();

    @BeforeEach
    public void setup() throws InitializationException {
        clients.clear();

        final ConsumeSlack processor = new ConsumeSlack() {
            @Override
            protected ConversationHistoryClient createConversationHistoryClient(final WebClientServiceProvider webClientServiceProvider, final String accessToken, final String channelId) {
                final MockConversationHistoryClient client = new MockConversationHistoryClient(channelId);
                clients.add(client);
                return client;
            }
        };

        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(ConsumeSlack.ACCESS_TOKEN, "token");
        testRunner.setProperty(ConsumeSlack.CHANNEL_IDS, "cid1");
        testRunner.setProperty(ConsumeSlack.BATCH_SIZE, "5");

        final WebClientServiceProvider webClientProvider = new StandardWebClientServiceProvider();
        testRunner.addControllerService("webClientServiceProvider", webClientProvider);
        testRunner.enableControllerService(webClientProvider);
        testRunner.setProperty(ConsumeSlack.WEB_CLIENT_SERVICE_PROVIDER, "webClientServiceProvider");
    }


    @Test
    public void testSuccessfullyReceivedSingleMessage() {
        final String message = createMessage("U12345", "Hello world", "1683903832.350");
        nextResponse = createSuccessResponse(wrapMessages(message));

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        final MockFlowFile outFlowFile1 = testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS).get(0);
        final String outputContent = outFlowFile1.getContent();
        assertEquals(stripSpaces(String.format("[%s]", message)), stripSpaces(outputContent));
        outFlowFile1.assertAttributeEquals("slack.channel.id", "cid1");
    }


    @Test
    public void testReceivedMultipleMessages() {
        final String message1 = createMessage("U12345", "Hello world", "1683903832.350");
        final String message2 = createMessage("U12345", "Good-bye", "1683903222.350");
        final String message3 = createMessage("U3333", "Hi there", "1683903442.410");
        final String message4 = createMessage("U0123", "Foo bar", "1683903832.520");
        nextResponse = createSuccessResponse(wrapMessages(message1, message2, message3, message4));

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        final MockFlowFile outFlowFile1 = testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS).get(0);
        final String outputContent = outFlowFile1.getContent();
        final String allMessages = String.join(", ", message1, message2, message3, message4);
        assertEquals(stripSpaces(String.format("[%s]", allMessages)), stripSpaces(outputContent));
        outFlowFile1.assertAttributeEquals("slack.channel.id", "cid1");
    }


    @Test
    public void testRateLimited() {
        nextResponse = createRateLimitedResponse(30);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 0);
        assertTrue(testRunner.isYieldCalled());
    }

    @Test
    public void testMultipleChannels() {
        testRunner.setProperty(ConsumeSlack.CHANNEL_IDS, "     cid1,     , cid2");
        final String message = createMessage("U12345", "Hello world", "1683903832.350");
        nextResponse = createSuccessResponse(wrapMessages(message));

        testRunner.run(1, false, true);
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        // Create another HttpResponse because the response can only be read once.
        nextResponse = createSuccessResponse(wrapMessages(message));
        testRunner.clearTransferState();
        testRunner.run(1, true, false);
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        // Ensure that 2 clients were created and that each was invoked once.
        assertEquals(2, clients.size());
        final Set<String> channelIds = new HashSet<>();
        for (final MockConversationHistoryClient client : clients) {
            assertEquals(1, client.getRequestCount());
            channelIds.add(client.getChannelId());
        }

        assertEquals(2, channelIds.size());
        assertTrue(channelIds.contains("cid1"));
        assertTrue(channelIds.contains("cid2"));
    }


    @Test
    public void testCursor() {
        final String timestamp = "4444444.444";
        final String message = createMessage("U12345", "Hello world", timestamp);
        nextResponse = createSuccessResponse(wrapMessages(message));

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        final MockFlowFile outFlowFile1 = testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS).get(0);
        final String outputContent = outFlowFile1.getContent();
        assertEquals(stripSpaces(String.format("[%s]", message)), stripSpaces(outputContent));
        outFlowFile1.assertAttributeEquals("slack.channel.id", "cid1");
        assertNull(clients.get(0).getLastCursor());

        nextResponse = createSuccessResponse(wrapMessages(message));
        testRunner.run();
        assertEquals(timestamp, clients.get(0).getLastCursor());
    }


    private String stripSpaces(final String input) {
        return input.replaceAll("\\s+", "");
    }

    private String createMessage(final String user, final String text, final String ts) {
        return String.format("""
            {
                "type": "message",
                "user": "%s",
                "text": "%s",
                "ts": "%s"
            }
            """, user, text, ts);
    }

    private String wrapMessages(final String... messages) {
        return String.format("""
            {
                "ok": true,
                "messages": [
                    %s
                ],
                "has_more": true
            }
            """, String.join(",", messages) );
    }

    private HttpResponse createSuccessResponse(final String message) {
        final HttpResponse response = mock(HttpResponse.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn(new ByteArrayInputStream(message.getBytes(StandardCharsets.UTF_8)));
        return response;
    }

    private HttpResponse createRateLimitedResponse(final int retryAfterSeconds) {
        final HttpResponse response = mock(HttpResponse.class);
        when(response.getStatusCode()).thenReturn(429);
        when(response.getResponseBody()).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(response.getHeader("Retry-After")).thenReturn(Optional.of(String.valueOf(retryAfterSeconds)));
        return response;
    }


    private class MockConversationHistoryClient implements ConversationHistoryClient {
        private final String channelId;
        private int requestCount = 0;
        private String lastCursor = null;

        public MockConversationHistoryClient(final String channelId) {
            this.channelId = channelId;
        }

        @Override
        public HttpResponse getConversationHistory(final String cursor, final int limit) {
            requestCount++;
            lastCursor = cursor;
            return nextResponse;
        }

        public String getLastCursor() {
            return lastCursor;
        }

        public int getRequestCount() {
            return requestCount;
        }

        @Override
        public String getChannelId() {
            return channelId;
        }

        @Override
        public void yield() {
        }

        @Override
        public boolean isYielded() {
            return false;
        }
    }
}
