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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = Scope.CLUSTER, description = "Maintains a mapping of Slack Channel IDs to the cursor or timestamp of the last message that was retrieved for that channel. " +
    "This allows the processor to only retrieve messages that have been posted since the last time the processor ran. " +
    "This state is stored in the cluster so that if the Primary Node changes, the new node will pick up where the previous node left off.")
@WritesAttributes({
    @WritesAttribute(attribute = "slack.channel.id", description = "The ID of the Slack Channel from which the messages were retrieved"),
    @WritesAttribute(attribute = "slack.message.count", description = "The number of slack messages that are included in the FlowFile"),
    @WritesAttribute(attribute = "mime.type", description = "Set to application/json, as the output will always be in JSON format")
})
@SeeAlso({PostSlack.class, PutSlack.class})
@Tags({"slack", "conversation", "conversation.history", "social media", "team", "text", "unstructured"})
@CapabilityDescription("Retrieves the latest messages from one or more configured Slack channels. The messages are written out in JSON format. See Usage / Additional Details for more information " +
    "about how to configure this Processor and enable it to retrieve messages from Slack.")
public class ConsumeSlack extends AbstractProcessor {

    private static final ObjectMapper objectMapper;
    static {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private static final String CONVERSATION_HISTORY_URL = "https://slack.com/api/conversations.history";

    static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
        .name("Web Client Service Provider")
        .description("The Controller Service that is used to create HTTP connections to Slack")
        .required(true)
        .identifiesControllerService(WebClientServiceProvider.class)
        .build();

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
        .name("Access Token")
        .description("OAuth Access Token used for authenticating/authorizing the Slack request sent by NiFi.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .sensitive(true)
        .build();

    static final PropertyDescriptor CHANNEL_IDS = new PropertyDescriptor.Builder()
        .name("Channel IDs")
        .description("A comma-separated list of IDs of the Slack Channels to Retrieve Messages From")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();


    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
        .name("Batch Size")
        .description("The maximum number of messages to retrieve in a single request to Slack. The entire response will be parsed into memory, " +
            "so it is important that this be kept in mind when setting this value.")
        .required(true)
        .addValidator(StandardValidators.createLongValidator(0, 1000, false))
        .defaultValue("100")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Slack messages that are successfully received will be routed to this relationship")
        .build();


    private final BlockingQueue<ConversationHistoryClient> clients = new LinkedBlockingDeque<>();
    private final AtomicLong nextRequestTime = new AtomicLong(0L);


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(WEB_CLIENT_SERVICE_PROVIDER, CHANNEL_IDS, ACCESS_TOKEN, BATCH_SIZE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        final String accessToken = context.getProperty(ACCESS_TOKEN).getValue();
        final String channelIdsValue = context.getProperty(CHANNEL_IDS).getValue();

        final WebClientServiceProvider webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);

        Arrays.stream(channelIdsValue.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .forEach(channelId -> {
                final ConversationHistoryClient client = createConversationHistoryClient(webClientServiceProvider, accessToken, channelId);
                clients.offer(client);
            });
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Check to see if we are currently in a backoff period due to Slack's Rate Limit
        if (isRateLimited()) {
            context.yield();
            return;
        }

        // Get a client for the next channel
        final ConversationHistoryClient client = getClient();
        if (client == null) {
            context.yield();
            return;
        }

        try {
            // Determine where we last left off for this channel
            final String channelId = client.getChannelId();

            final StateMap stateMap;
            final String channelOldest;
            try {
                stateMap = session.getState(Scope.CLUSTER);
                channelOldest = stateMap.get(channelId);
            } catch (final IOException ioe) {
                getLogger().error("Failed to determine current offset for channel {}; will not retrieve any messages until this is resolved", channelId, ioe);
                context.yield();
                return;
            }

            // Retrieve the next batch of messages and write them to FlowFiles.
            final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
            try (final HttpResponse slackResponse = client.getConversationHistory(channelOldest, batchSize)) {
                final ConversationHistoryResponse response = parseResponse(slackResponse);
                if (!response.isOk()) {
                    getLogger().error("Received unexpected response from Slack when attempting to retrieve messages for channel {}: {}", channelId, response.getError());
                    context.yield();
                    return;
                }

                final ArrayNode messagesArray = response.getMessages();
                if (messagesArray.size() == 0) {
                    getLogger().debug("Received no new messages from Slack for channel {}", channelId);
                    client.yield();
                    return;
                }

                // Write the results out to a FlowFile
                FlowFile flowFile = session.create();
                flowFile = session.write(flowFile, out -> {
                    try (final JsonGenerator generator = objectMapper.createGenerator(out)) {
                        generator.writeStartArray();
                        for (final JsonNode message : messagesArray) {
                            generator.writeTree(message);
                        }
                        generator.writeEndArray();
                    }
                });

                // Determine attributes for outbound FlowFile
                final Map<String, String> attributes = new HashMap<>();
                attributes.put("slack.channel.id", channelId);
                attributes.put("slack.message.count", Integer.toString(response.getMessages().size()));
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");

                final boolean cursorUsed = channelOldest != null && !UnixTimestamp.isTimestamp(channelOldest);
                if (channelOldest != null) {
                    final String offsetKey = cursorUsed ? "slack.cursor" : "slack.oldest";
                    attributes.put(offsetKey, channelOldest);
                }

                // Update provenance
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.getProvenanceReporter().receive(flowFile, CONVERSATION_HISTORY_URL);
                session.transfer(flowFile, REL_SUCCESS);

                if (!response.hasMore()) {
                    client.yield();
                }

                // Update state based on the next cursor, preferably, or the timestamp of the last message if either we didn't get back
                // a cursor, or if we got back a cursor but our original query was made using the 'oldest' parameter.
                final String stateValue;
                final String nextCursor = response.getNextCursor();
                final boolean cursorIgnored = channelOldest != null && !cursorUsed;
                if (cursorIgnored || nextCursor == null) {
                    final UnixTimestamp latest = response.getLatest();
                    stateValue = latest == null ? null : latest.toString();
                } else {
                    stateValue = nextCursor;
                }

                if (stateValue == null) {
                    getLogger().debug("Could not determine latest timestamp when retrieving messages for channel {}", channelId);
                } else {
                    final Map<String, String> updatedStateMap = new HashMap<>(stateMap.toMap());
                    updatedStateMap.put(channelId, stateValue);
                    session.setState(updatedStateMap, Scope.CLUSTER);
                }
            } catch (final RateLimitExceededException rlee) {
                getLogger().warn("Slack indicated that the Rate Limit has been exceeded when attempting to retrieve messages for channel {}", channelId);
                final long timeOfNextRequest = System.currentTimeMillis() + (rlee.getRetrySeconds() * 1000L);
                nextRequestTime.getAndUpdate(currentTime -> Math.max(currentTime, timeOfNextRequest));
                context.yield();
                return;
            } catch (final Exception e) {
                getLogger().error("Failed to retrieve messages for channel {}", channelId, e);
                context.yield();
                return;
            }
        } finally {
            clients.offer(client);
        }
    }


    private boolean isRateLimited() {
        final long nextTime = nextRequestTime.get();
        if (nextTime > 0 && System.currentTimeMillis() < nextTime) {
            getLogger().debug("Will not retrieve any messages until {} due to Slack's Rate Limit", new Date(nextTime));
            return true;
        } else if (nextTime > 0) {
            // Set nextRequestTime to 0 so that we no longer bother to make system calls to System.currentTimeMillis()
            nextRequestTime.compareAndSet(nextTime, 0);
        }

        return false;
    }

    private ConversationHistoryClient getClient() {
        final List<ConversationHistoryClient> yieldedClients = new ArrayList<>();

        try {
            while (!clients.isEmpty()) {
                final ConversationHistoryClient client = clients.poll();
                if (client == null) {
                    return null;
                }

                if (client.isYielded()) {
                    yieldedClients.add(client);
                    continue;
                }

                return client;
            }
        } finally {
            clients.addAll(yieldedClients);
        }

        return null;
    }


    protected ConversationHistoryClient createConversationHistoryClient(final WebClientServiceProvider webClientServiceProvider, final String accessToken, final String channelId) {
        return new StandardConversationHistoryClient(webClientServiceProvider, accessToken, channelId);
    }

    private static class UnixTimestamp implements Comparable<UnixTimestamp> {
        private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("\\d+\\.\\d+");

        private final long seconds;
        private final long fraction;

        public UnixTimestamp(final long seconds, final long fraction) {
            this.seconds = seconds;
            this.fraction = fraction;
        }

        public String toString() {
            return seconds + "." + fraction;
        }

        public static boolean isTimestamp(final String value) {
            return TIMESTAMP_PATTERN.matcher(value).matches();
        }

        @Override
        public int compareTo(final UnixTimestamp other) {
            final int secondsResult = Long.compare(seconds, other.seconds);
            if (secondsResult != 0) {
                return secondsResult;
            }

            return Long.compare(fraction, other.fraction);
        }

        public static UnixTimestamp parse(final String value) {
            if (!isTimestamp(value)) {
                throw new IllegalArgumentException("Value is not a valid timestamp: " + value);
            }

            final int index = value.indexOf(".");
            final String before = value.substring(0, index);
            final String after = value.substring(index + 1);
            return new UnixTimestamp(Long.parseLong(before), Long.parseLong(after));
        }
    }

    private interface ConversationHistoryResponse {
        boolean isOk();

        String getError();

        String getNextCursor();

        UnixTimestamp getLatest();

        boolean hasMore();

        ArrayNode getMessages();
    }

    private ConversationHistoryResponse parseResponse(final HttpResponse slackResponse) throws IOException, RateLimitExceededException {
        final int statusCode = slackResponse.getStatusCode();
        if (statusCode == 429) {
            final Optional<String> retryAfter = slackResponse.getHeader("Retry-After");
            final int retrySeconds = retryAfter.map(Integer::parseInt).orElse(60);
            throw new RateLimitExceededException(retrySeconds);
        } else if (statusCode != 200) {
            return new ErrorResponse("Received unexpected status code " + statusCode + " from Slack: " + slackResponse.getResponseBody());
        }


        try (final JsonParser jsonParser = objectMapper.createParser(slackResponse.getResponseBody())) {
            final JsonNode jsonNode = jsonParser.readValueAsTree();
            if (jsonNode == null) {
                return new ErrorResponse("Response was empty");
            }

            final JsonNode okNode = jsonNode.get("ok");
            if (okNode == null) {
                return new ErrorResponse("Message did not contain 'ok' element");
            }

            final boolean ok = okNode.booleanValue();
            if (!ok) {
                final JsonNode errorNode = jsonNode.get("error");
                String error = "<No error message provided>";
                if (errorNode != null) {
                    error = errorNode.textValue();
                }

                return new ErrorResponse(error);
            }

            String nextCursor = null;
            final JsonNode responseMetadataNode = jsonNode.get("response_metadata");
            if (responseMetadataNode != null) {
                final JsonNode nextCursorNode = responseMetadataNode.get("next_cursor");
                if (nextCursorNode != null) {
                    nextCursor = nextCursorNode.textValue();
                }
            }

            final JsonNode hasMoreNode = jsonNode.get("has_more");
            final boolean hasMore = hasMoreNode == null || hasMoreNode.booleanValue();

            final JsonNode messages = jsonNode.get("messages");
            final ArrayNode messagesArray = (ArrayNode) messages;

            // Find the latest timestamp. Based on looking at the records returned, it appears that the latest timestamp
            // will be always be the first message in the array (as opposed to the last, which I would have assumed).
            // However, I cannot find any documentation from Slack that guarantees this, so we just iterate through all of
            // the messages to find the latest timestamp. Otherwise, we'll keep pulling the same messages over and over.
            UnixTimestamp latestTimestamp = null;
            for (final JsonNode message : messagesArray) {
                final String tsValue = message.get("ts").textValue().trim();

                final UnixTimestamp parsed = UnixTimestamp.parse(tsValue);
                if (latestTimestamp == null || parsed.compareTo(latestTimestamp) > 0) {
                    latestTimestamp = parsed;
                }
            }

            return new SuccessfulResponse(messagesArray, nextCursor, hasMore, latestTimestamp);
        }
    }


    private static class ErrorResponse implements ConversationHistoryResponse {
        private final String error;

        public ErrorResponse(final String error) {
            this.error = error;
        }

        @Override
        public boolean isOk() {
            return false;
        }

        @Override
        public String getError() {
            return error;
        }

        @Override
        public String getNextCursor() {
            return null;
        }

        @Override
        public UnixTimestamp getLatest() {
            return null;
        }

        @Override
        public boolean hasMore() {
            return false;
        }

        @Override
        public ArrayNode getMessages() {
            return null;
        }
    }

    private static class SuccessfulResponse implements ConversationHistoryResponse {
        private final ArrayNode messages;
        private final String nextCursor;
        private final boolean hasMore;
        private final UnixTimestamp latest;

        public SuccessfulResponse(final ArrayNode messages, final String nextCursor, final boolean hasMore, final UnixTimestamp oldest) {
            this.messages = messages;
            this.nextCursor = nextCursor;
            this.hasMore = hasMore;
            this.latest = oldest;
        }

        @Override
        public boolean isOk() {
            return true;
        }

        @Override
        public String getError() {
            return null;
        }

        @Override
        public String getNextCursor() {
            return nextCursor;
        }

        @Override
        public boolean hasMore() {
            return hasMore;
        }

        @Override
        public ArrayNode getMessages() {
            return messages;
        }

        @Override
        public UnixTimestamp getLatest() {
            return latest;
        }
    }


    public static class RateLimitExceededException extends Exception {
        private final int retrySeconds;

        public RateLimitExceededException(final int retrySeconds) {
            super();
            this.retrySeconds = retrySeconds;
        }

        public int getRetrySeconds() {
            return retrySeconds;
        }
    }


    protected interface HttpResponse extends Closeable {
        int getStatusCode();

        InputStream getResponseBody();

        Optional<String> getHeader(String headerName);
    }

    /**
     * An interface for making a request to Slack's API to retrieve a channel's conversation history.
     * The response from this method is a String containing the JSON response from Slack.
     * It makes no attempt to parse or validate the response.
     * While this could be performed inline in the processor, creating a separate interface and implementation for this
     * yields cleaner code by separating the concerns of interacting with Slack and processing the response.
     * This also allows for easier unit testing of the processor.
     */
    public interface ConversationHistoryClient {
        HttpResponse getConversationHistory(String cursor, int limit);

        String getChannelId();

        void yield();

        boolean isYielded();
    }

    private static class StandardConversationHistoryClient implements ConversationHistoryClient {
        private static final long YIELD_MILLIS = 3_000L;
        private final WebClientServiceProvider webClientServiceProvider;
        private final String accessToken;
        private final String channelId;
        private volatile long yieldExpiration = 0L;

        public StandardConversationHistoryClient(final WebClientServiceProvider webClientServiceProvider, final String accessToken, final String channelId) {
            this.webClientServiceProvider = webClientServiceProvider;
            this.accessToken = accessToken;
            this.channelId = channelId;
        }

        public String getChannelId() {
            return channelId;
        }

        @Override
        public void yield() {
            yieldExpiration = System.currentTimeMillis() + YIELD_MILLIS;
        }

        @Override
        public boolean isYielded() {
            final long expiration = yieldExpiration;
            return expiration > 0L && System.currentTimeMillis() < expiration;
        }

        @Override
        public HttpResponse getConversationHistory(final String cursor, final int limit) {
            yieldExpiration = 0L;   // reset the yield expiration

            final WebClientService webClientService = webClientServiceProvider.getWebClientService();

            HttpUriBuilder uriBuilder = webClientServiceProvider.getHttpUriBuilder()
                .scheme("https")
                .host("slack.com")
                .addPathSegment("api")
                .addPathSegment("conversations.history")
                .addQueryParameter("channel", channelId)
                .addQueryParameter("limit", String.valueOf(limit));

            // The Slack API provides two different ways of performing pagination. The first is by using a 'cursor',
            // which is preferred but only is made available when iterating over historical data. When we reach the end,
            // there is no cursor to tell us where we've left off. So, in order to continue iterating, we need to use
            // the 'oldest' parameter, which is a timestamp in the form of #####.####
            if (cursor != null) {
                if (UnixTimestamp.isTimestamp(cursor)) {
                    uriBuilder = uriBuilder.addQueryParameter("oldest", cursor);
                    uriBuilder = uriBuilder.addQueryParameter("inclusive", "false");
                } else {
                    uriBuilder = uriBuilder.addQueryParameter("cursor", cursor);
                }
            }

            final URI uri = uriBuilder.build();
            final HttpResponseEntity entity = webClientService.get()
                        .uri(uri)
                        .header("Authorization", "Bearer " + accessToken)
                        .header("Accept", "application/json")
                        .retrieve();

            final HttpResponse response = new HttpResponse() {
                @Override
                public void close() throws IOException {
                    entity.close();
                }

                @Override
                public int getStatusCode() {
                    return entity.statusCode();
                }

                @Override
                public InputStream getResponseBody() {
                    return entity.body();
                }

                @Override
                public Optional<String> getHeader(final String headerName) {
                    return entity.headers().getFirstHeader(headerName);
                }
            };

            return response;
        }
    }
}
