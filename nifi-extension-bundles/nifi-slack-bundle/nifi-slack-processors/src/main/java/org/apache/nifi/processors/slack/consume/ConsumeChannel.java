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
package org.apache.nifi.processors.slack.consume;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.conversations.ConversationsHistoryRequest;
import com.slack.api.methods.request.conversations.ConversationsRepliesRequest;
import com.slack.api.methods.response.conversations.ConversationsHistoryResponse;
import com.slack.api.methods.response.conversations.ConversationsRepliesResponse;
import com.slack.api.model.Message;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.slack.util.SlackResponseUtil;

public class ConsumeChannel {
    private static final String CONVERSATION_HISTORY_URL = "https://slack.com/api/conversations.history";

    private static final String CHECK_FOR_REPLIES = "check for replies";
    private static final String BACKWARD = "backward";
    private static final String FORWARD = "forward";
    private static final Pattern MENTION_PATTERN = Pattern.compile("<@(U.*?)>");
    private static final long YIELD_MILLIS = 3_000L;


    private final ConsumeSlackClient client;
    private final String channelId;
    private final String channelName;
    private final int batchSize;
    private final long replyMonitorFrequencyMillis;
    private final long replyMonitorWindowMillis;
    private final boolean resolveUsernames;
    private final boolean includeMessageBlocks;
    private final UsernameLookup usernameLookup;
    private final Relationship successRelationship;
    private final ComponentLog logger;
    private final ObjectMapper objectMapper;
    private final StateKeys stateKeys;

    private volatile long yieldExpiration;
    private volatile long lastReplyMonitorPollEnd = System.currentTimeMillis();
    private final AtomicLong nextRequestTime = new AtomicLong(0L);


    private ConsumeChannel(final Builder builder) {
        this.client = builder.client;
        this.channelId = builder.channelId;
        this.channelName = builder.channelName;
        this.batchSize = builder.batchSize;
        this.replyMonitorFrequencyMillis = builder.replyMonitorFrequencyMillis;
        this.replyMonitorWindowMillis = builder.replyMonitorWindowMillis;
        this.logger = builder.logger;
        this.resolveUsernames = builder.resolveUsernames;
        this.includeMessageBlocks = builder.includeMessageBlocks;
        this.successRelationship = builder.successRelationship;
        this.usernameLookup = builder.usernameLookup;
        this.objectMapper = builder.objectMapper;

        stateKeys = new StateKeys(channelId);
    }

    public String getChannelId() {
        return channelId;
    }

    public ConfigVerificationResult verify() {
        final ConversationsHistoryRequest request = ConversationsHistoryRequest.builder()
            .channel(channelId)
            .limit(1)
            .build();

        final ConversationsHistoryResponse response;
        try {
            response = client.fetchConversationsHistory(request);
        } catch (final Exception e) {
            return new ConfigVerificationResult.Builder()
                .verificationStepName("Check authorization for Channel " + channelId)
                .outcome(ConfigVerificationResult.Outcome.FAILED)
                .explanation("Failed to obtain a message due to: " + e)
                .build();
        }

        if (response.isOk()) {
            final List<Message> messages = response.getMessages();
            final Message firstMessage = messages.get(0);
            enrichMessage(firstMessage);

            final String username = firstMessage.getUsername();
            if (resolveUsernames && username == null) {
                return new ConfigVerificationResult.Builder()
                    .verificationStepName("Check authorization for Channel " + channelId)
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation("Successfully retrieved a message but failed to resolve the username")
                    .build();
            }

            final String user = username == null ? firstMessage.getUser() : username;
            final String explanation = response.getMessages().isEmpty() ? "Successfully requested messages for channel but got no messages" : "Successfully retrieved a message from " + user;

            return new ConfigVerificationResult.Builder()
                .verificationStepName("Check authorization for Channel " + channelId)
                .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                .explanation(explanation)
                .build();
        }

        final String errorMessage = SlackResponseUtil.getErrorMessage(response.getError(), response.getNeeded(), response.getProvided(), response.getWarning());
        return new ConfigVerificationResult.Builder()
            .verificationStepName("Check authorization for Channel " + channelId)
            .outcome(ConfigVerificationResult.Outcome.FAILED)
            .explanation("Failed to obtain a message due to: " + errorMessage)
            .build();
    }

    public void consume(final ProcessContext context, final ProcessSession session) throws IOException, SlackApiException {
        final long minTimestamp = nextRequestTime.get();
        if (minTimestamp > 0 && System.currentTimeMillis() < minTimestamp) {
            context.yield();
            return;
        }

        // Get the current state
        final StateMap stateMap;
        try {
            stateMap = session.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            logger.error("Failed to determine current offset for channel {}; will not retrieve any messages until this is resolved", channelId, ioe);
            context.yield();
            return;
        }

        // Determine if we need to check historical messages for replies, or if we need to consume the latest messages.
        final boolean checkForReplies = isCheckForReplies(stateMap);

        if (checkForReplies) {
            consumeReplies(context, session, stateMap);
        } else {
            consumeLatestMessages(context, session, stateMap);
        }
    }

    private boolean isCheckForReplies(final StateMap stateMap) {
        final String currentAction = stateMap.get(stateKeys.ACTION);
        if (CHECK_FOR_REPLIES.equals(currentAction)) {
            return true;
        }

        final long nextCheckRepliesTime = lastReplyMonitorPollEnd + replyMonitorFrequencyMillis;
        if (System.currentTimeMillis() > nextCheckRepliesTime) {
            return true;
        }

        return false;
    }

    private void consumeReplies(final ProcessContext context, final ProcessSession session, final StateMap stateMap) throws IOException, SlackApiException {
        // Make sure that we've completed our initial "load" of messages. If not, we want to load the messages before we start
        // monitoring for updates to threads.
        final String direction = stateMap.get(stateKeys.DIRECTION);
        if (!FORWARD.equals(direction)) {
            onCompletedRepliesScan(session, new HashMap<>(stateMap.toMap()), null);
            return;
        }

        // We want to use the latest timestamp we've seen as the threshold for replies.
        final String latestTs = stateMap.get(stateKeys.LATEST_TS);
        if (latestTs == null) {
            onCompletedRepliesScan(session, new HashMap<>(stateMap.toMap()), null);
            return;
        }

        // If the action has not been set to denote that we're in teh process of checking for replies, do so now.
        final Map<String, String> updatedStateMap = new HashMap<>(stateMap.toMap());
        final String currentAction = stateMap.get(stateKeys.ACTION);
        if (!CHECK_FOR_REPLIES.equals(currentAction)) {
            updatedStateMap.put(stateKeys.ACTION, CHECK_FOR_REPLIES);
            session.setState(updatedStateMap, Scope.CLUSTER);
        }

        String minTsValue = stateMap.get(stateKeys.REPLY_MIN_TS);
        if (minTsValue == null) {
            minTsValue = latestTs;
        }
        final String maxTsValue = stateMap.get(stateKeys.REPLY_MAX_TS);
        final SlackTimestamp minTs = new SlackTimestamp(minTsValue);
        final SlackTimestamp maxTs = maxTsValue == null ? new SlackTimestamp() : new SlackTimestamp(maxTsValue);
        final SlackTimestamp maxParentTs = new SlackTimestamp(latestTs);

        final String oldestThreadTs = new SlackTimestamp(System.currentTimeMillis() - replyMonitorWindowMillis).getRawValue();
        String earliestThreadTs = stateMap.get(stateKeys.HISTORICAL_REPLIES_EARLIEST_THREAD_TS);
        if (earliestThreadTs == null) {
            earliestThreadTs = new SlackTimestamp(System.currentTimeMillis()).getRawValue();
        }
        String repliesCursor = stateMap.get(stateKeys.HISTORICAL_MESSAGES_REPLIES_CURSOR);

        while (true) {
            final ConversationsHistoryRequest request = ConversationsHistoryRequest.builder()
                .channel(channelId)
                .limit(500)
                .latest(earliestThreadTs)
                .oldest(oldestThreadTs)
                .inclusive(true)
                .build();

            // Never write the messages because we are only interested in replies
            final Predicate<Message> messageFilter = message -> false;

            final Predicate<Message> replyFilter = reply -> {
                final SlackTimestamp replyTs = new SlackTimestamp(reply.getTs());

                // If the timestamp of the reply is newer than our cutoff, don't include it.
                if (replyTs.afterOrEqualTo(maxTs)) {
                    return false;
                }

                // If the timestamp is before our min timestamp, we should have already output it.
                if (replyTs.beforeOrEqualTo(minTs)) {
                    return false;
                }

                // If the parent message of the thread is newer than the latest parent message we've listed, ignore it.
                // We'll output it the next time we output parent messages.
                final SlackTimestamp replyThreadTs = new SlackTimestamp(reply.getThreadTs());
                if (replyThreadTs.after(maxParentTs)) {
                    return false;
                }

                return true;
            };

            final ConsumptionResults results = consumeMessages(context, session, request, messageFilter, repliesCursor, minTs, replyFilter);

            // If finished consuming replies, remove all keys related to this action
            if (!results.isMore() && !results.isFailure()) {
                onCompletedRepliesScan(session, updatedStateMap, maxTs);
                return;
            }

            final SlackTimestamp earliest = results.getEarliestTimestamp();
            earliestThreadTs = earliest == null ? null : earliest.getRawValue();
            repliesCursor = results.getRepliesCursor();

            if (earliestThreadTs == null) {
                break;
            }

            // Update state
            updatedStateMap.put(stateKeys.HISTORICAL_REPLIES_EARLIEST_THREAD_TS, earliestThreadTs);
            if (repliesCursor != null) {
                updatedStateMap.put(stateKeys.HISTORICAL_MESSAGES_REPLIES_CURSOR, repliesCursor);
            }

            session.setState(updatedStateMap, Scope.CLUSTER);
            session.commitAsync();

            if (!results.isContinuePolling()) {
                break;
            }
        }
    }

    private void onCompletedRepliesScan(final ProcessSession session, final Map<String, String> updatedStateMap, final SlackTimestamp replyTsCutoff) throws IOException {
        updatedStateMap.remove(stateKeys.ACTION);
        updatedStateMap.remove(stateKeys.HISTORICAL_REPLIES_EARLIEST_THREAD_TS);
        updatedStateMap.remove(stateKeys.HISTORICAL_MESSAGES_REPLIES_CURSOR);
        updatedStateMap.remove(stateKeys.REPLY_MAX_TS);

        if (replyTsCutoff != null) {
            updatedStateMap.put(stateKeys.REPLY_MIN_TS, replyTsCutoff.getRawValue());
        }

        session.setState(updatedStateMap, Scope.CLUSTER);
        lastReplyMonitorPollEnd = System.currentTimeMillis();
    }

    private void consumeLatestMessages(final ProcessContext context, final ProcessSession session, final StateMap stateMap) throws IOException, SlackApiException {
        final String startingRepliesCursor = stateMap.get(stateKeys.LATEST_REPLIES_CURSOR);

        String direction = stateMap.get(stateKeys.DIRECTION);
        if (direction == null) {
            direction = BACKWARD;
        }

        final String startingTimestampKey = BACKWARD.equals(direction) ? stateKeys.EARLIEST_TS : stateKeys.LATEST_TS;
        String ts = stateMap.get(startingTimestampKey);

        // If there's a cursor for replies, we want to include the last message as a duplicate, so that we can easily
        // fetch its replies. We'll not write out the message itself, as it was already written in a previous FlowFile.
        boolean includeLastMessage = startingRepliesCursor != null;
        String repliesCursor = startingRepliesCursor;
        final Map<String, String> updatedStateMap = new HashMap<>(stateMap.toMap());

        while (true) {
            // The logic here for building these requests gets complex, unfortunately
            // When a request is made to Slack to retrieve Conversation History, it allows specifying
            // the max number of messages and an oldest and/or latest timestamp. However, the messages that
            // are returned are always in the order of newest to oldest. So, if we were to set the oldest timestamp
            // to say 0, with a limit of 5, we would get the 5 newest messages - not the 5 oldest.
            // But when the Processor first runs, we want the ability to retrieve all messages from a given channel.
            // Because of that, we have to set the 'latest' timestamp on the request. Unfortunately, there's
            // no way to start at the beginning and progress forward to the current time. So, instead, we must start
            // at the current time and progress backward until we reach the oldest message in the channel. This is done
            // by setting the latest timestamp to null, initially, and with every batch of messages received, update that
            // to the earliest timestamp seen.
            //
            // However, once we've reached the beginning of the Channel history, we want to begin now only listing newer
            // messages. So we set the direction to 'forward'. We then set the timestamp in the request such that there is
            // no 'latest' and the 'oldest' timestamp is set to the most recent timestamp we've seen.
            // So during the 'initial load' of messages we have to keep track of the direction (so that we know in the next
            // invocation that we've not yet finished the initial load), the earliest timestamp that we've seen (so that
            // we can continue going further back in history), and the latest timestamp that we've seen (so that we know
            // where to start off once we finish going back and start going forward).
            final ConversationsHistoryRequest request = ConversationsHistoryRequest.builder()
                .channel(channelId)
                .limit(batchSize)
                .inclusive(includeLastMessage)
                .build();

            if (direction.equals(FORWARD)) {
                request.setOldest(ts);
                request.setLatest(null);
            } else {
                request.setOldest(null);
                request.setLatest(ts);
            }

            final String firstMessageTs = ts;
            final Predicate<Message> messageFilter = message -> !Objects.equals(message.getTs(), firstMessageTs);
            final Predicate<Message> replyFilter = message -> true; // Include all replies

            final ConsumptionResults results = consumeMessages(context, session, request, messageFilter, repliesCursor, null, replyFilter);

            final String timestampKeyName;
            final SlackTimestamp resultTimestamp;
            if (direction.equals(FORWARD)) {
                resultTimestamp = results.getLatestTimestamp();
                timestampKeyName = stateKeys.LATEST_TS;
            } else {
                resultTimestamp = results.getEarliestTimestamp();
                timestampKeyName = stateKeys.EARLIEST_TS;
            }
            if (resultTimestamp == null) {
                break;
            }

            // Update state
            ts = resultTimestamp.getRawValue();
            repliesCursor = results.getRepliesCursor();
            includeLastMessage = repliesCursor != null;

            updatedStateMap.put(timestampKeyName, ts);

            // If the latest timestamp hasn't yet been set, set it. This allows us to know the latest timestamp when
            // we switch the direction from BACKWARD to FORWARD.
            if (updatedStateMap.get(stateKeys.LATEST_TS) == null) {
                final SlackTimestamp latestTimestamp = results.getLatestTimestamp();
                updatedStateMap.put(stateKeys.LATEST_TS, latestTimestamp == null ? null : latestTimestamp.getRawValue());
            }

            if (repliesCursor != null) {
                updatedStateMap.put(stateKeys.LATEST_REPLIES_CURSOR, repliesCursor);
            }

            // Set the direction to forward only once we reach the end of all messages
            if (!results.isMore() && !results.isFailure()) {
                updatedStateMap.put(stateKeys.DIRECTION, FORWARD);

                // This key is only relevant during the initial loading of messages, when direction is BACKWARD.
                updatedStateMap.remove(stateKeys.EARLIEST_TS);
                logger.info("Successfully completed initial load of messages for channel {}", channelId);
            }

            session.setState(updatedStateMap, Scope.CLUSTER);
            session.commitAsync();

            if (!results.isContinuePolling()) {
                break;
            }
        }
    }


    private ConsumptionResults consumeMessages(final ProcessContext context, final ProcessSession session, final ConversationsHistoryRequest request, final Predicate<Message> messageFilter,
                                               final String startingRepliesCursor, final SlackTimestamp oldestReplyTs, final Predicate<Message> replyFilter) throws IOException, SlackApiException {

        // Gather slack conversation history
        final ConversationsHistoryResponse response = client.fetchConversationsHistory(request);
        if (!response.isOk()) {
            final String error = SlackResponseUtil.getErrorMessage(response.getError(), response.getNeeded(), response.getProvided(), response.getWarning());
            logger.error("Received unexpected response from Slack when attempting to retrieve messages for channel {}: {}", channelId, error);
            context.yield();
            return new StandardConsumptionResults(null, null, null, true, false, false);
        }

        // If no messages, we're done.
        final List<Message> messages = response.getMessages();
        if (messages.isEmpty()) {
            logger.debug("Received no new messages from Slack for channel {}", channelId);
            this.yield();
            return new StandardConsumptionResults(null, null, null, false, false, false);
        }

        // Write the results out to a FlowFile. This includes optionally gathering the threaded messages / replies.
        FlowFile flowFile = session.create();
        int messageCount = 0;
        PartialThreadException partialThreadException = null;

        SlackTimestamp earliestTimestamp = null;
        SlackTimestamp latestTimestamp = null;

        try (final OutputStream out = session.write(flowFile);
             final JsonGenerator generator = objectMapper.createGenerator(out)) {

            generator.writeStartArray();

            final Iterator<Message> messageItr = messages.iterator();
            while (messageItr.hasNext()) {
                final Message message = messageItr.next();

                // Do not include the message if it's the channel oldest. In this case, we only have fetched it
                // in order to make the code simpler so that we can just handle the next section, where we
                // deal with replies.
                boolean enrichFailed = false;
                final boolean includeMessage = messageFilter.test(message);
                if (includeMessage) {
                    // Slack appears to populate the 'team' field but not the channel for Messages for some reason. We need the channel to be populated
                    // in order to fetch replies, and it makes sense to have it populated, regardless. So we populate it ourselves.
                    final boolean success = enrichMessage(message);
                    enrichFailed = !success;

                    generator.writeObject(message);
                    messageCount++;

                    final SlackTimestamp msgTimestamp = new SlackTimestamp(message.getTs());
                    if (earliestTimestamp == null || msgTimestamp.before(earliestTimestamp)) {
                        earliestTimestamp = msgTimestamp;
                    }
                    if (latestTimestamp == null || msgTimestamp.after(latestTimestamp)) {
                        latestTimestamp = msgTimestamp;
                    }
                } else {
                    messageItr.remove();
                }

                // Simple case is that we need to output only the message.
                if (!SlackResponseUtil.hasReplies(message)) {
                    continue;
                }

                // Gather replies for the message. We handle the case of PartialThreadException
                // carefully because we may well be rate limited, or there could be a server error, etc.
                // In this case, we want to output the messages that were received, and keep track of the
                // cursor so that we can gather the next chunk in the next iteration.
                List<Message> replies;
                try {
                    replies = fetchReplies(message, startingRepliesCursor, oldestReplyTs);
                } catch (final PartialThreadException e) {
                    yieldOnException(e, channelId, message, context);

                    partialThreadException = e;
                    replies = e.getRetrieved();
                }

                // Write out each of the replies as an individual JSON message
                for (final Message reply : replies) {
                    // The first message in the thread is the message itself. We don't want to include it again,
                    // but we check the timestamp to ensure that this is the case, since the documentation does
                    // not explicitly call this out.
                    if (reply.getTs().equals(message.getTs())) {
                        continue;
                    }

                    if (replyFilter.test(reply)) {
                        final boolean success = enrichMessage(reply);
                        enrichFailed = enrichFailed || !success;

                        generator.writeObject(reply);
                    }
                }

                messageCount += replies.size();

                // If we encountered an Exception while pulling Threaded messages, or couldn't perform enrichment (which generally means rate limiting),
                // stop iterating through messages and remove the rest from the list of Messages. This allows us to properly keep track of the offsets, etc.
                if (partialThreadException != null || enrichFailed) {
                    while (messageItr.hasNext()) {
                        messageItr.next();
                        messageItr.remove();
                    }

                    break;
                }
            }

            generator.writeEndArray();
        }

        if (!response.isHasMore()) {
            this.yield();
        }

        if (messageCount == 0) {
            session.remove(flowFile);

            // We consider there to be more messages if an PartialThreadException was thrown, as it means there may be additional
            // messages in the thread.
            final boolean moreMessages = partialThreadException != null || response.isHasMore();
            return new StandardConsumptionResults(null, null, null, partialThreadException != null, false, moreMessages);
        }

        // Determine attributes for outbound FlowFile
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("slack.channel.id", channelId);
        attributes.put("slack.channel.name", channelName);
        attributes.put("slack.message.count", Integer.toString(messageCount));
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");

        // Update provenance
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().receive(flowFile, CONVERSATION_HISTORY_URL);
        session.transfer(flowFile, successRelationship);

        // Update state based on the next cursor, preferably, or the timestamp of the last message if either we didn't get back
        // a cursor, or if we got back a cursor but our original query was made using the 'oldest' parameter.
        String repliesCursor = null;
        if (partialThreadException != null) {
            repliesCursor = partialThreadException.getNextCursor();
        }

        final boolean hasMoreReplies = repliesCursor != null;
        final boolean moreMessages = response.isHasMore() || hasMoreReplies;
        final boolean continuePolling = partialThreadException == null && moreMessages;
        return new StandardConsumptionResults(earliestTimestamp, latestTimestamp, repliesCursor, partialThreadException != null, continuePolling, moreMessages);
    }


    private boolean enrichMessage(final Message message) {
        message.setChannel(channelId);

        if (!includeMessageBlocks) {
            message.setBlocks(null);
        }

        final AtomicBoolean lookupFailed = new AtomicBoolean(false);
        if (resolveUsernames) {
            if (message.getUsername() == null && message.getUser() != null) {
                final String username = usernameLookup.getUsername(message.getUser());
                if (username == null) {
                    lookupFailed.set(true);
                }

                message.setUsername(username);
            }

            final String text = message.getText();
            if (text != null) {
                final Matcher matcher = MENTION_PATTERN.matcher(text);
                final String updatedText = matcher.replaceAll(matchResult -> {
                    final String id = matchResult.group(1);
                    final String username = usernameLookup.getUsername(id);
                    if (username == null) {
                        lookupFailed.set(true);
                        matchResult.group(0);
                    }

                    return "<@" + username + ">";
                });

                message.setText(updatedText);
            }
        }

        return !lookupFailed.get();
    }


    private void yieldOnException(final PartialThreadException e, final String channelId, final Message message, final ProcessContext context) {
        if (SlackResponseUtil.isRateLimited(e.getCause())) {
            final int retryAfterSeconds = SlackResponseUtil.getRetryAfterSeconds(e);
            logger.warn("Slack indicated that the Rate Limit has been exceeded when attempting to retrieve messages for channel {}; will continue in {} seconds",
                channelId, retryAfterSeconds);
        } else {
            logger.error("Encountered unexpected response from Slack when retrieving replies to message with thread timestamp {} due to: {}",
                message.getThreadTs(), e.getMessage(), e);
        }

        final int retryAfterSeconds = SlackResponseUtil.getRetryAfterSeconds(e);
        final long timeOfNextRequest = System.currentTimeMillis() + (retryAfterSeconds * 1000L);
        nextRequestTime.getAndUpdate(currentTime -> Math.max(currentTime, timeOfNextRequest));
        context.yield();
    }



    private List<Message> fetchReplies(final Message message, final String startCursor, final SlackTimestamp oldestTs) throws SlackApiException, IOException, PartialThreadException {
        final List<Message> replies = new ArrayList<>();

        // If the message's latest reply is before our cutoff, don't bother polling for replies
        if (oldestTs != null) {
            final String latestReply = message.getLatestReply();
            if (latestReply != null && new SlackTimestamp(latestReply).before(oldestTs)) {
                return Collections.emptyList();
            }
        }

        String cursor = startCursor;
        while (true) {
            final ConversationsRepliesRequest request = ConversationsRepliesRequest.builder()
                .channel(channelId)
                .ts(message.getThreadTs())
                .includeAllMetadata(true)
                .limit(1000)
                .oldest(oldestTs == null ? null : oldestTs.getRawValue())
                .cursor(cursor)
                .build();

            final ConversationsRepliesResponse response;

            try {
                response = client.fetchConversationsReplies(request);
            } catch (final Exception e) {
                if (replies.isEmpty()) {
                    throw e;
                }

                throw new PartialThreadException(replies, cursor, e);
            }

            if (!response.isOk()) {
                final String errorMessage = SlackResponseUtil.getErrorMessage(response.getError(), response.getNeeded(), response.getProvided(), response.getWarning());
                throw new PartialThreadException(replies, cursor, errorMessage);
            }

            replies.addAll(response.getMessages());
            if (!response.isHasMore()) {
                break;
            }

            cursor = response.getResponseMetadata().getNextCursor();
        }

        return replies;
    }

    public void yield() {
        yieldExpiration = System.currentTimeMillis() + YIELD_MILLIS;
    }

    public boolean isYielded() {
        final long expiration = this.yieldExpiration;
        if (expiration == 0) {
            return false;
        }

        if (System.currentTimeMillis() < yieldExpiration) {
            return true;
        }

        // Reset yield expiration to 0 so that next time we don't need to make the system call to get current time.
        yieldExpiration = 0L;
        return false;
    }



    public static class Builder {
        private ConsumeSlackClient client;
        private String channelId;
        private String channelName;
        private boolean includeMessageBlocks;
        private boolean resolveUsernames;
        private int batchSize = 50;
        private ComponentLog logger;
        private Relationship successRelationship;
        private UsernameLookup usernameLookup;
        private long replyMonitorFrequencyMillis = TimeUnit.SECONDS.toMillis(60);
        private long replyMonitorWindowMillis = TimeUnit.DAYS.toMillis(7);
        private ObjectMapper objectMapper;

        public Builder channelId(final String channelId) {
            this.channelId = channelId;
            return this;
        }

        public Builder channelName(final String channelName) {
            this.channelName = channelName;
            return this;
        }

        public Builder client(final ConsumeSlackClient client) {
            this.client = client;
            return this;
        }

        public Builder batchSize(final int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder logger(final ComponentLog logger) {
            this.logger = logger;
            return this;
        }

        public Builder replyMonitorFrequency(final long value, final TimeUnit timeUnit) {
            this.replyMonitorFrequencyMillis = timeUnit.toMillis(value);
            return this;
        }

        public Builder replyMonitorWindow(final long value, final TimeUnit timeUnit) {
            this.replyMonitorWindowMillis = timeUnit.toMillis(value);
            return this;
        }

        public Builder includeMessageBlocks(final boolean includeMessageBlocks) {
            this.includeMessageBlocks = includeMessageBlocks;
            return this;
        }

        public Builder resolveUsernames(final boolean resolveUsernames) {
            this.resolveUsernames = resolveUsernames;
            return this;
        }

        public Builder successRelationship(final Relationship relationship) {
            this.successRelationship = relationship;
            return this;
        }

        public Builder usernameLookup(final UsernameLookup lookup) {
            this.usernameLookup = lookup;
            return this;
        }

        public Builder objectMapper(final ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        public ConsumeChannel build() {
            return new ConsumeChannel(this);
        }
    }


    private interface ConsumptionResults {
        SlackTimestamp getEarliestTimestamp();

        SlackTimestamp getLatestTimestamp();

        String getRepliesCursor();

        boolean isFailure();

        boolean isContinuePolling();

        boolean isMore();
    }


    private static class StandardConsumptionResults implements ConsumptionResults {
        private final SlackTimestamp earliestTimestamp;
        private final SlackTimestamp latestTimestamp;
        private final boolean failure;
        private final boolean continuePolling;
        private final String repliesCursor;
        private final boolean isMore;

        public StandardConsumptionResults(final SlackTimestamp earliestTimestamp, final SlackTimestamp latestTimestamp, final String repliesCursor,
                                          final boolean failure, final boolean continuePolling, final boolean moreMessages) {
            this.earliestTimestamp = earliestTimestamp;
            this.latestTimestamp = latestTimestamp;
            this.repliesCursor = repliesCursor;
            this.failure = failure;
            this.continuePolling = continuePolling;
            this.isMore = moreMessages;
        }

        @Override
        public SlackTimestamp getEarliestTimestamp() {
            return earliestTimestamp;
        }

        @Override
        public SlackTimestamp getLatestTimestamp() {
            return latestTimestamp;
        }

        @Override
        public String getRepliesCursor() {
            return repliesCursor;
        }

        @Override
        public boolean isFailure() {
            return failure;
        }

        @Override
        public boolean isContinuePolling() {
            return continuePolling;
        }

        @Override
        public boolean isMore() {
            return isMore;
        }
    }


    private static class StateKeys {
        public final String ACTION;
        public final String LATEST_TS;
        public final String EARLIEST_TS;
        public final String DIRECTION;
        public final String LATEST_REPLIES_CURSOR;
        public final String HISTORICAL_MESSAGES_REPLIES_CURSOR;
        public final String HISTORICAL_REPLIES_EARLIEST_THREAD_TS;
        public final String REPLY_MIN_TS;
        public final String REPLY_MAX_TS;

        public StateKeys(final String channelId) {
            ACTION = channelId + ".action";
            LATEST_TS = channelId + ".latest";
            EARLIEST_TS = channelId + ".earliest";
            DIRECTION = channelId + ".direction";
            LATEST_REPLIES_CURSOR = channelId + ".latest.replies.cursor";
            HISTORICAL_MESSAGES_REPLIES_CURSOR = channelId + ".historical.replies.cursor";
            HISTORICAL_REPLIES_EARLIEST_THREAD_TS = channelId + ".historical.replies.ts";
            REPLY_MIN_TS = channelId + ".historical.reply.min.ts";
            REPLY_MAX_TS = channelId + ".historical.reply.max.ts";
        }
    }
}
