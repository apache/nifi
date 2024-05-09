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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.api.bolt.App;
import com.slack.api.methods.request.conversations.ConversationsHistoryRequest;
import com.slack.api.methods.request.conversations.ConversationsRepliesRequest;
import com.slack.api.methods.response.conversations.ConversationsHistoryResponse;
import com.slack.api.methods.response.conversations.ConversationsRepliesResponse;
import com.slack.api.methods.response.users.UsersInfoResponse;
import com.slack.api.model.Message;
import com.slack.api.model.ResponseMetadata;
import com.slack.api.model.User;
import org.apache.nifi.processors.slack.consume.ConsumeSlackClient;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConsumeSlack {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private TestRunner testRunner;
    private MockConsumeSlackClient client;
    ConsumeSlack processor;

    @BeforeEach
    public void setup() {
        client = new MockConsumeSlackClient();

        // Create an instance of the processor that mocks out the initialize() method to return a client we can use for testing
        processor = new ConsumeSlack() {
            @Override
            protected ConsumeSlackClient initializeClient(final App slackApp) {
                return client;
            }
        };

        testRunner = TestRunners.newTestRunner(processor);
        testRunner.setProperty(ConsumeSlack.ACCESS_TOKEN, "token");
        testRunner.setProperty(ConsumeSlack.CHANNEL_IDS, "cid1");
        testRunner.setProperty(ConsumeSlack.BATCH_SIZE, "5");
    }

    @Test
    public void testRequestRateLimited() {
        testRunner.setProperty(ConsumeSlack.CHANNEL_IDS, "cid1,#cname2");
        final Message message = createMessage("U12345", "Hello world", "1683903832.350");
        client.addHistoryResponse(noMore(createSuccessfulHistoryResponse(message)));

        testRunner.run(1, false, true);
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);
        testRunner.clearTransferState();

        // Create another HttpResponse because each response can only be read once.
        client.addHistoryResponse(noMore(createSuccessfulHistoryResponse(message)));
        // Set processor to be in rate limited state, therefore it will process 0 flowfiles
        processor.getRateLimit().retryAfter(Duration.ofSeconds(30));
        testRunner.run(1, true, false);
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 0);
    }

    @Test
    public void testSuccessfullyReceivedSingleMessage() throws JsonProcessingException {
        final Message message = createMessage("U12345", "Hello world", "1683903832.350");
        final ConversationsHistoryResponse response = createSuccessfulHistoryResponse(message);
        client.addHistoryResponse(response);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        final MockFlowFile outFlowFile1 = testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS).get(0);
        final String outputContent = outFlowFile1.getContent();

        final Message[] outputMessages = objectMapper.readValue(outputContent, Message[].class);
        assertEquals(message, outputMessages[0]);

        outFlowFile1.assertAttributeEquals("slack.channel.id", "cid1");
        outFlowFile1.assertAttributeEquals("slack.channel.name", "#cname1");
    }


    @Test
    public void testReceivedMultipleMessages() throws JsonProcessingException {
        final Message message1 = createMessage("U12345", "Hello world", "1683903832.350");
        final Message message2 = createMessage("U12345", "Good-bye", "1683903222.350");
        final Message message3 = createMessage("U3333", "Hi there", "1683903442.410");
        final Message message4 = createMessage("U0123", "Foo bar", "1683903832.520");

        final ConversationsHistoryResponse response = createSuccessfulHistoryResponse(message1, message2, message3, message4);
        client.addHistoryResponse(response);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        final MockFlowFile outFlowFile1 = testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS).get(0);
        final String outputContent = outFlowFile1.getContent();
        final Message[] outputMessages = objectMapper.readValue(outputContent, Message[].class);
        final Message[] expectedMessages = new Message[] {message1, message2, message3, message4};
        assertArrayEquals(expectedMessages, outputMessages);

        outFlowFile1.assertAttributeEquals("slack.channel.id", "cid1");
        outFlowFile1.assertAttributeEquals("slack.channel.name", "#cname1");
    }


    @Test
    public void testYieldOnException() {
        client.rateLimitOnNextCall(30);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 0);
        assertTrue(testRunner.isYieldCalled());
    }

    @Test
    public void testMultipleChannels() {
        testRunner.setProperty(ConsumeSlack.CHANNEL_IDS, "     cid1,     , cid2");
        final Message message = createMessage("U12345", "Hello world", "1683903832.350");
        client.addHistoryResponse(noMore(createSuccessfulHistoryResponse(message)));

        testRunner.run(1, false, true);
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        // Create another HttpResponse because the response can only be read once.
        client.addHistoryResponse(noMore(createSuccessfulHistoryResponse(message)));
        testRunner.clearTransferState();
        testRunner.run(1, true, false);
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        // Ensure that only 2 requests were made and that each of them had a different channel id
        final List<ConversationsHistoryRequest> requestsMade = client.getHistoryRequestsMade();
        assertEquals(2, requestsMade.size());
        final List<String> channelIdsRequested = requestsMade.stream()
            .map(ConversationsHistoryRequest::getChannel)
            .distinct()
            .toList();

        assertEquals(2, channelIdsRequested.size());
        assertTrue(channelIdsRequested.contains("cid1"));
        assertTrue(channelIdsRequested.contains("cid2"));
    }

    @Test
    public void testRequestsNextTimestampWithoutStopping() {
        final Message message = createMessage("U12345", "Hello world", "1683903832.350");

        final ConversationsHistoryResponse response = createSuccessfulHistoryResponse(message);
        client.addHistoryResponse(response);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        final List<ConversationsHistoryRequest> requestsMade = client.getHistoryRequestsMade();
        assertEquals(2, requestsMade.size());
        assertNull(requestsMade.get(0).getOldest());
        assertEquals("1683903832.350", requestsMade.get(1).getLatest());
    }

    @Test
    public void testRequestsNextTimestampIfStoppedInBetween() {
        final Message message = createMessage("U12345", "Hello world", "1683903832.350");

        final ConversationsHistoryResponse response = noMore(createSuccessfulHistoryResponse(message));
        client.addHistoryResponse(response);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        testRunner.run();

        final List<ConversationsHistoryRequest> requestsMade = client.getHistoryRequestsMade();
        assertEquals(2, requestsMade.size());
        assertNull(requestsMade.get(0).getOldest());
        assertEquals("1683903832.350", requestsMade.get(1).getOldest());
    }


    @Test
    public void testMessageWithReplies() throws JsonProcessingException {
        final String threadTs = "3820387.29201";
        final Message message1 = createMessage("U12345", "hello", "1683903822.55");
        final Message message2 = createMessage("U12345", "Hello world", "1683903832.350", threadTs);
        final Message message3 = createMessage("U12345", "hello", "1683904421.55");

        final List<Message> replies = new ArrayList<>();
        for (int i=0; i < 3; i++) {
            final Message reply = createMessage("U1234" + i, "Hello world " + i, String.valueOf(System.currentTimeMillis()), threadTs);
            replies.add(reply);
        }

        final ConversationsRepliesResponse repliesResponse = createSuccessfulRepliesResponse(replies);
        client.addConversationsReplies(threadTs, repliesResponse);

        final ConversationsHistoryResponse response = createSuccessfulHistoryResponse(message1, message2, message3);
        client.addHistoryResponse(response);

        testRunner.setProperty(ConsumeSlack.INCLUDE_NULL_FIELDS, "false");
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        // Verify the results
        final MockFlowFile outFlowFile1 = testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS).get(0);
        final String outputContent = outFlowFile1.getContent();
        final Message[] outputMessages = objectMapper.readValue(outputContent, Message[].class);
        final Message[] expectedMessages = new Message[] {message1, message2, replies.get(0), replies.get(1), replies.get(2), message3};
        assertArrayEquals(expectedMessages, outputMessages);

        outFlowFile1.assertAttributeEquals("slack.message.count", Integer.toString(expectedMessages.length));

        // Ensure that a request was made to gather additional messages
        final List<ConversationsRepliesRequest> requestsMade = client.getRepliesRequestsMade();
        assertEquals(2, requestsMade.size());

        final long noCursorRequestCount = requestsMade.stream()
            .filter(request -> request.getCursor() == null)
            .count();

        final long nextCursorRequestCount = requestsMade.stream()
            .filter(request -> "next".equals(request.getCursor()))
            .count();

        assertEquals(1L, noCursorRequestCount);
        assertEquals(1L, nextCursorRequestCount);
    }


    @Test
    public void testRateLimitedWhileFetchingReplies() throws InterruptedException, JsonProcessingException {
        // Enqueue 3 messages with the second one (only) having replies.
        final String threadTs = "1683903832.350";
        final Message message1 = createMessage("U12345", "hello", "1683903832.550");
        final Message message2 = createMessage("U12345", "Hello world", threadTs, threadTs);
        final Message message3 = createMessage("U12345", "hello", "1683903832.250");

        final List<Message> replies = new ArrayList<>();
        for (int i=0; i < 3; i++) {
            final Message reply = createMessage("U1234" + i, "Hello world " + i, String.valueOf(System.currentTimeMillis()), threadTs);
            replies.add(reply);
        }

        final ConversationsRepliesResponse repliesResponse = createSuccessfulRepliesResponse(replies);
        client.addConversationsReplies(threadTs, repliesResponse);

        // Configure client such that it will respond with a Rate Limiting exception on the second call to gather replies
        final ConversationsHistoryResponse response = createSuccessfulHistoryResponse(message1, message2, message3);
        client.addHistoryResponse(response);
        client.rateLimitWhenRepliesEmpty(1);

        // Trigger the Processor to run.
        testRunner.run(1, false, true);

        // We expect 1 output FlowFile, which should contain messages 1 and 2 and the 3 replies (checked below). But
        // we should also find that the context was yielded because of the Rate Limit being encountered.
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);
        assertTrue(testRunner.isYieldCalled());

        // Add the the second and third message back so that they will be retrieved again.
        final ConversationsHistoryResponse secondResponse = createSuccessfulHistoryResponse(message2, message3);
        client.addHistoryResponse(secondResponse);

        // Include one additional reply so that we can ensure that we do continue pulling replies from the last message that we were gathering from
        // when we encountered the Rate Limit. We expect the first call to include a cursor because it should be continuing from where it left off
        // in the last call to onTrigger.
        final ConversationsRepliesResponse secondReplies = createSuccessfulRepliesResponse(replies.subList(2, 3));
        client.addConversationsReplies(threadTs, secondReplies, CursorExpectation.EXPECT_CURSOR);

        // Run until we get output. The processor will immediately determine at the beginning of onTrigger that
        // it should not request messages for the channel again because the channel has been yielded. So we just
        // keep triggering it to run until the channel is no longer yielded.
        while (testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS).size() < 2) {
            testRunner.run(1, false, false);
            Thread.sleep(10L);
        }

        // Call run() so that the @OnStopped logic will run
        testRunner.run();

        // Ensure that we have exactly 2 output FlowFiles.
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 2);

        // Verify output of first FlowFile
        final List<MockFlowFile> outputFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS);
        final String firstOutputContent = outputFlowFiles.get(0).getContent();
        final Message[] firstOutputMessages = objectMapper.readValue(firstOutputContent, Message[].class);
        final Message[] expectedFirstMessages = new Message[] {message1, message2, replies.get(0), replies.get(1), replies.get(2)};
        assertArrayEquals(expectedFirstMessages, firstOutputMessages);

        // Verify output of the second FlowFile
        final String secondOutputContent = outputFlowFiles.get(1).getContent();
        final Message[] secondOutputMessages = objectMapper.readValue(secondOutputContent, Message[].class);
        final Message[] expectedSecondMessages = new Message[] {replies.get(2), message3};
        assertArrayEquals(expectedSecondMessages, secondOutputMessages);
    }

    @Test
    public void testUsernameResolution() throws JsonProcessingException {
        final Message message = createMessage("U12345", "Hello <@U12345>!", "1683903832.350");
        final ConversationsHistoryResponse response = createSuccessfulHistoryResponse(message);
        client.addHistoryResponse(response);

        client.addUserMapping("U12345", "nifi");

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);

        final MockFlowFile outFlowFile1 = testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS).get(0);
        final String outputContent = outFlowFile1.getContent();

        final Message[] outputMessages = objectMapper.readValue(outputContent, Message[].class);
        final Message outputMessage = outputMessages[0];
        assertEquals("Hello <@nifi>!", outputMessage.getText());
        assertEquals("nifi", outputMessage.getUsername());
        assertEquals("U12345", outputMessage.getUser());
    }

    @Test
    public void testNotOkHistoryResponse() {
        final String errorText = "Unit Test Intentional Error";
        final ConversationsHistoryResponse response = createNotOkHistoryResponse(errorText);
        client.addHistoryResponse(response);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 0);

        final long errorMessageCount = testRunner.getLogger().getErrorMessages().stream()
            .filter(msg -> msg.getMsg().contains(errorText))
            .count();
        assertEquals(1L, errorMessageCount);
        assertTrue(testRunner.isYieldCalled());
    }


    @Test
    public void testNotOkResponseWhileFetchingReplies() throws InterruptedException, JsonProcessingException {
        // Enqueue 3 messages with the second one (only) having replies.
        final String threadTs = "1683903832.350";
        final Message message1 = createMessage("U12345", "hello", "1683903832.550");
        final Message message2 = createMessage("U12345", "Hello world", threadTs, threadTs);
        final Message message3 = createMessage("U12345", "hello", "1683903832.250");

        final List<Message> replies = new ArrayList<>();
        for (int i=0; i < 3; i++) {
            final Message reply = createMessage("U1234" + i, "Hello world " + i, String.valueOf(System.currentTimeMillis()), threadTs);
            replies.add(reply);
        }

        final String errorText = "Intentional Unit Test Error";
        final ConversationsRepliesResponse repliesResponse = createNotOkRepliesResponse(errorText);
        client.addConversationsReplies(threadTs, repliesResponse);

        // Configure client such that it will respond with a Rate Limiting exception on the second call to gather replies
        final ConversationsHistoryResponse response = createSuccessfulHistoryResponse(message1, message2, message3);
        client.addHistoryResponse(response);

        // Trigger the Processor to run.
        testRunner.run(1, false, true);

        // We expect 1 output FlowFile, which should contain messages 1 and 2 and the 3 replies (checked below). But
        // we should also find that the context was yielded because of the Rate Limit being encountered.
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 1);
        assertTrue(testRunner.isYieldCalled());
        final long errorMessageCount = testRunner.getLogger().getErrorMessages().stream()
            .filter(message -> message.getMsg().contains(errorText))
            .count();
        assertEquals(1, errorMessageCount);

        // Add the the second and third message back so that they will be retrieved again.
        final ConversationsHistoryResponse secondResponse = createSuccessfulHistoryResponse(message2, message3);
        client.addHistoryResponse(secondResponse);

        // Include one additional reply so that we can ensure that we do continue pulling replies from the last message that we were gathering from
        // when we encountered the Rate Limit. We expect the first call to include a cursor because it should be continuing from where it left off
        // in the last call to onTrigger.
        final ConversationsRepliesResponse secondReplies = createSuccessfulRepliesResponse(replies.subList(2, 3));
        client.addConversationsReplies(threadTs, secondReplies);

        // Run until we get output. The processor will immediately determine at the beginning of onTrigger that
        // it should not request messages for the channel again because the channel has been yielded. So we just
        // keep triggering it to run until the channel is no longer yielded.
        while (testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS).size() < 2) {
            testRunner.run(1, false, false);
            Thread.sleep(10L);
        }

        // Call run() so that the @OnStopped logic will run
        testRunner.run();

        // Ensure that we have exactly 2 output FlowFiles.
        testRunner.assertAllFlowFilesTransferred(ConsumeSlack.REL_SUCCESS, 2);

        // Verify output of first FlowFile
        final List<MockFlowFile> outputFlowFiles = testRunner.getFlowFilesForRelationship(ConsumeSlack.REL_SUCCESS);
        final String firstOutputContent = outputFlowFiles.get(0).getContent();
        final Message[] firstOutputMessages = objectMapper.readValue(firstOutputContent, Message[].class);
        final Message[] expectedFirstMessages = new Message[] {message1, message2};
        assertArrayEquals(expectedFirstMessages, firstOutputMessages);

        // Verify output of the second FlowFile
        final String secondOutputContent = outputFlowFiles.get(1).getContent();
        final Message[] secondOutputMessages = objectMapper.readValue(secondOutputContent, Message[].class);
        final Message[] expectedSecondMessages = new Message[] {replies.get(2), message3};
        assertArrayEquals(expectedSecondMessages, secondOutputMessages);
    }


    private Message createMessage(final String user, final String text, final String ts) {
        return createMessage(user, text, ts, null);
    }

    private Message createMessage(final String user, final String text, final String ts, final String threadTs) {
        final Message message = new Message();
        message.setUser(user);
        message.setText(text);
        message.setType("message");
        message.setTs(ts);
        message.setThreadTs(threadTs);
        return message;
    }

    private ConversationsHistoryResponse noMore(final ConversationsHistoryResponse response) {
        response.setHasMore(false);
        return response;
    }

    private ConversationsHistoryResponse createSuccessfulHistoryResponse(final Message... messages) {
        return createSuccessfulHistoryResponse(Arrays.asList(messages));
    }

    private ConversationsHistoryResponse createSuccessfulHistoryResponse(final List<Message> messages) {
        final ConversationsHistoryResponse response = new ConversationsHistoryResponse();
        response.setOk(true);
        response.setMessages(new ArrayList<>(messages));
        response.setHasMore(true);
        return response;
    }

    public ConversationsHistoryResponse createNotOkHistoryResponse(final String error) {
        final ConversationsHistoryResponse response = new ConversationsHistoryResponse();
        response.setOk(false);
        response.setError(error);
        return response;
    }

    private ConversationsRepliesResponse createSuccessfulRepliesResponse(final List<Message> messages) {
        final ConversationsRepliesResponse response = new ConversationsRepliesResponse();
        response.setOk(true);
        response.setMessages(new ArrayList<>(messages));
        response.setHasMore(true);

        final ResponseMetadata metadata = new ResponseMetadata();
        metadata.setNextCursor("next");
        response.setResponseMetadata(metadata);

        return response;
    }

    private ConversationsRepliesResponse createNotOkRepliesResponse(final String errorMessage) {
        final ConversationsRepliesResponse response = new ConversationsRepliesResponse();
        response.setOk(false);
        response.setError(errorMessage);
        return response;
    }


    private static class MockConsumeSlackClient implements ConsumeSlackClient {
        private static final ConversationsHistoryResponse EMPTY_HISTORY_RESPONSE;
        private static final ConversationsRepliesResponse EMPTY_REPLIES_RESPONSE;

        static {
            EMPTY_HISTORY_RESPONSE = new ConversationsHistoryResponse();
            EMPTY_HISTORY_RESPONSE.setOk(true);
            EMPTY_HISTORY_RESPONSE.setMessages(Collections.emptyList());
            EMPTY_HISTORY_RESPONSE.setHasMore(false);

            EMPTY_REPLIES_RESPONSE = new ConversationsRepliesResponse();
            EMPTY_REPLIES_RESPONSE.setOk(true);
            EMPTY_REPLIES_RESPONSE.setMessages(Collections.emptyList());
            EMPTY_REPLIES_RESPONSE.setHasMore(false);

            final ResponseMetadata metadata = new ResponseMetadata();
            metadata.setNextCursor(null);
            metadata.setMessages(Collections.emptyList());
            EMPTY_REPLIES_RESPONSE.setResponseMetadata(metadata);
        }

        private final Queue<ConversationsHistoryResponse> historyResponses = new LinkedBlockingQueue<>();
        private final Map<String, ConversationsRepliesResponse> repliesResponses = new HashMap<>();
        private final AtomicInteger retryAfterSeconds = new AtomicInteger();
        private final AtomicInteger retryAfterSecondsWhenRepliesEmpty = new AtomicInteger();
        private CursorExpectation cursorExpectation = CursorExpectation.NO_EXPECTATION;

        private final List<ConversationsHistoryRequest> historyRequestsMade = new ArrayList<>();
        private final List<ConversationsRepliesRequest> repliesRequestsMade = new ArrayList<>();
        private final Map<String, String> userIdToNameMapping = new HashMap<>();

        @Override
        public ConversationsHistoryResponse fetchConversationsHistory(final ConversationsHistoryRequest request) {
            historyRequestsMade.add(request);

            checkRateLimit();

            final ConversationsHistoryResponse response = historyResponses.poll();
            return Objects.requireNonNullElse(response, EMPTY_HISTORY_RESPONSE);
        }

        @Override
        public ConversationsRepliesResponse fetchConversationsReplies(final ConversationsRepliesRequest request) {
            repliesRequestsMade.add(request);
            checkRateLimit();

            final String cursor = request.getCursor();
            if (cursor != null && cursorExpectation == CursorExpectation.EXPECT_NO_CURSOR) {
                throw new RuntimeException("Expected No Cursor to be provided but received a cursor of " + cursor);
            } else if (cursor == null && cursorExpectation == CursorExpectation.EXPECT_CURSOR) {
                throw new RuntimeException("Expected Cursor to be provided but received no cursor");
            }

            final String ts = request.getTs();
            final ConversationsRepliesResponse response = repliesResponses.remove(ts);

            if (response == null) {
                final int retryAfter = retryAfterSecondsWhenRepliesEmpty.getAndSet(0);
                if (retryAfter > 0) {
                    throw new RateLimitedException(retryAfter);
                }
            }

            return Objects.requireNonNullElse(response, EMPTY_REPLIES_RESPONSE);
        }

        @Override
        public UsersInfoResponse fetchUsername(final String userId) {
            final String username = userIdToNameMapping.get(userId);

            final User user = new User();
            user.setName(Objects.requireNonNullElse(username, userId));

            final UsersInfoResponse response = new UsersInfoResponse();
            response.setOk(true);
            response.setUser(user);
            return response;
        }

        public void addUserMapping(final String userId, final String username) {
            userIdToNameMapping.put(userId, username);
        }

        @Override
        public Map<String, String> fetchChannelIds() {
            final Map<String, String> nameIdMapping = new HashMap<String, String>();
            nameIdMapping.put("#cname1", "cid1");
            nameIdMapping.put("#cname2", "cid2");
            return nameIdMapping;
        }

        @Override
        public String fetchChannelName(String channelId) {
            Map<String, String> invertedMap = fetchChannelIds().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
            return invertedMap.get(channelId);
        }

        private void checkRateLimit() {
            final int seconds = retryAfterSeconds.getAndSet(0);
            if (seconds <= 0) {
                return;
            }

            throw new RateLimitedException(seconds);
        }


        public void addHistoryResponse(final ConversationsHistoryResponse response) {
            historyResponses.offer(response);
        }

        public void addConversationsReplies(final String timestamp, final ConversationsRepliesResponse response) {
            repliesResponses.put(timestamp, response);
        }

        public void addConversationsReplies(final String timestamp, final ConversationsRepliesResponse response, final CursorExpectation cursorExpectation) {
            addConversationsReplies(timestamp, response);
            this.cursorExpectation = cursorExpectation;
        }

        public void rateLimitOnNextCall(final int seconds) {
            retryAfterSeconds.set(seconds);
        }

        public void rateLimitWhenRepliesEmpty(final int seconds) {
            retryAfterSecondsWhenRepliesEmpty.set(seconds);
        }

        public List<ConversationsHistoryRequest> getHistoryRequestsMade() {
            return historyRequestsMade;
        }

        public List<ConversationsRepliesRequest> getRepliesRequestsMade() {
            return repliesRequestsMade;
        }
    }

    private static class RateLimitedException extends RuntimeException {
        private final int retryAfterSeconds;

        public RateLimitedException(final int retryAfterSeconds) {
            this.retryAfterSeconds = retryAfterSeconds;
        }

        public int getRetryAfterSeconds() {
            return retryAfterSeconds;
        }
    }

    private enum CursorExpectation {
        EXPECT_CURSOR,
        EXPECT_NO_CURSOR,
        NO_EXPECTATION;
    }
}
