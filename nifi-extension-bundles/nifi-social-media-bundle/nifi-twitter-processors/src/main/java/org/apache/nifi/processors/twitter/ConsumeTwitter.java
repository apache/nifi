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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@PrimaryNodeOnly
@SupportsBatching
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"twitter", "tweets", "social media", "status", "json"})
@CapabilityDescription("Streams tweets from Twitter's streaming API v2. The stream provides a sample stream or a search "
    + "stream based on previously uploaded rules. This processor also provides a pass through for certain fields of the "
    + "tweet to be returned as part of the response. See "
    + "https://developer.twitter.com/en/docs/twitter-api/data-dictionary/introduction for more information regarding the "
    + "Tweet object model.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type set to application/json"),
        @WritesAttribute(attribute = "tweets", description = "The number of Tweets in the FlowFile"),
})
public class ConsumeTwitter extends AbstractProcessor {

    static final AllowableValue ENDPOINT_SAMPLE = new AllowableValue(StreamEndpoint.SAMPLE_ENDPOINT.getEndpointName(),
            "Sample Stream",
            "Streams about one percent of all Tweets. " +
                    "https://developer.twitter.com/en/docs/twitter-api/tweets/volume-streams/api-reference/get-tweets-sample-stream");
    static final AllowableValue ENDPOINT_SEARCH = new AllowableValue(StreamEndpoint.SEARCH_ENDPOINT.getEndpointName(),
            "Search Stream",
            "The search stream produces Tweets that match filtering rules configured on Twitter services. " +
                    "At least one well-formed filtering rule must be configured. " +
                    "https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/api-reference/get-tweets-search-stream");

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor.Builder()
            .name("stream-endpoint")
            .displayName("Stream Endpoint")
            .description("The source from which the processor will consume Tweets.")
            .required(true)
            .allowableValues(ENDPOINT_SAMPLE, ENDPOINT_SEARCH)
            .defaultValue(ENDPOINT_SAMPLE.getValue())
            .build();
    public static final PropertyDescriptor BASE_PATH = new PropertyDescriptor.Builder()
            .name("base-path")
            .displayName("Base Path")
            .description("The base path that the processor will use for making HTTP requests. " +
                    "The default value should be sufficient for most use cases.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("https://api.twitter.com")
            .build();
    public static final PropertyDescriptor BEARER_TOKEN = new PropertyDescriptor.Builder()
            .name("bearer-token")
            .displayName("Bearer Token")
            .description("The Bearer Token provided by Twitter.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("queue-size")
            .displayName("Queue Size")
            .description("Maximum size of internal queue for streamed messages")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("The maximum size of the number of Tweets to be written to a single FlowFile. " +
                    "Will write fewer Tweets based on the number available in the queue at the time of processor invocation.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1000")
            .build();
    public static final PropertyDescriptor BACKOFF_ATTEMPTS = new PropertyDescriptor.Builder()
            .name("backoff-attempts")
            .displayName("Backoff Attempts")
            .description("The number of reconnection tries the processor will attempt in the event of " +
                    "a disconnection of the stream for any reason, before throwing an exception. To start a stream after " +
                    "this exception occur and the connection is fixed, please stop and restart the processor. If the value" +
                    "of this property is 0, then backoff will never occur and the processor will always need to be restarted" +
                    "if the stream fails.")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();
    public static final PropertyDescriptor BACKOFF_TIME = new PropertyDescriptor.Builder()
            .name("backoff-time")
            .displayName("Backoff Time")
            .description("The duration to backoff before requesting a new stream if" +
                    "the current one fails for any reason. Will increase by factor of 2 every time a restart fails")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 mins")
            .build();
    public static final PropertyDescriptor MAXIMUM_BACKOFF_TIME = new PropertyDescriptor.Builder()
            .name("maximum-backoff-time")
            .displayName("Maximum Backoff Time")
            .description("The maximum duration to backoff to start attempting a new stream." +
                    "It is recommended that this number be much higher than the 'Backoff Time' property")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 mins")
            .build();
    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connect-timeout")
            .displayName("Connect Timeout")
            .description("The maximum time in which client should establish a connection with the " +
                    "Twitter API before a time out. Setting the value to 0 disables connection timeouts.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 secs")
            .build();
    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("read-timeout")
            .displayName("Read Timeout")
            .description("The maximum time of inactivity between receiving tweets from Twitter through " +
                    "the API before a timeout. Setting the value to 0 disables read timeouts.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 secs")
            .build();
    public static final PropertyDescriptor BACKFILL_MINUTES = new PropertyDescriptor.Builder()
            .name("backfill-minutes")
            .displayName("Backfill Minutes")
            .description("The number of minutes (up to 5 minutes) of streaming data to be requested after a " +
                    "disconnect. Only available for project with academic research access. See " +
                    "https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/" +
                    "recovery-and-redundancy-features")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor TWEET_FIELDS = new PropertyDescriptor.Builder()
            .name("tweet-fields")
            .displayName("Tweet Fields")
            .description("A comma-separated list of tweet fields to be returned as part of the tweet. Refer to " +
                    "https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet " +
                    "for proper usage. Possible field values include: " +
                    "attachments, author_id, context_annotations, conversation_id, created_at, entities, geo, id, " +
                    "in_reply_to_user_id, lang, non_public_metrics, organic_metrics, possibly_sensitive, promoted_metrics, " +
                    "public_metrics, referenced_tweets, reply_settings, source, text, withheld")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USER_FIELDS = new PropertyDescriptor.Builder()
            .name("user-fields")
            .displayName("User Fields")
            .description("A comma-separated list of user fields to be returned as part of the tweet. Refer to " +
                    "https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/user " +
                    "for proper usage. Possible field values include: " +
                    "created_at, description, entities, id, location, name, pinned_tweet_id, profile_image_url, " +
                    "protected, public_metrics, url, username, verified, withheld")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor MEDIA_FIELDS = new PropertyDescriptor.Builder()
            .name("media-fields")
            .displayName("Media Fields")
            .description("A comma-separated list of media fields to be returned as part of the tweet. Refer to " +
                    "https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/media " +
                    "for proper usage. Possible field values include: " +
                    "alt_text, duration_ms, height, media_key, non_public_metrics, organic_metrics, preview_image_url, " +
                    "promoted_metrics, public_metrics, type, url, width")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor POLL_FIELDS = new PropertyDescriptor.Builder()
            .name("poll-fields")
            .displayName("Poll Fields")
            .description("A comma-separated list of poll fields to be returned as part of the tweet. Refer to " +
                    "https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/poll " +
                    "for proper usage. Possible field values include: " +
                    "duration_minutes, end_datetime, id, options, voting_status")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PLACE_FIELDS = new PropertyDescriptor.Builder()
            .name("place-fields")
            .displayName("Place Fields")
            .description("A comma-separated list of place fields to be returned as part of the tweet. Refer to " +
                    "https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/place " +
                    "for proper usage. Possible field values include: " +
                    "contained_within, country, country_code, full_name, geo, id, name, place_type")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor EXPANSIONS = new PropertyDescriptor.Builder()
            .name("expansions")
            .displayName("Expansions")
            .description("A comma-separated list of expansions for objects in the returned tweet. See " +
                    "https://developer.twitter.com/en/docs/twitter-api/expansions " +
                    "for proper usage. Possible field values include: " +
                    "author_id, referenced_tweets.id, referenced_tweets.id.author_id, entities.mentions.username, " +
                    "attachments.poll_ids, attachments.media_keys ,in_reply_to_user_id, geo.place_id")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing an array of one or more Tweets")
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
        ENDPOINT,
        BASE_PATH,
        BEARER_TOKEN,
        QUEUE_SIZE,
        BATCH_SIZE,
        BACKOFF_ATTEMPTS,
        BACKOFF_TIME,
        MAXIMUM_BACKOFF_TIME,
        CONNECT_TIMEOUT,
        READ_TIMEOUT,
        BACKFILL_MINUTES,
        TWEET_FIELDS,
        USER_FIELDS,
        MEDIA_FIELDS,
        POLL_FIELDS,
        PLACE_FIELDS,
        EXPANSIONS
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private TweetStreamService tweetStreamService;

    private volatile BlockingQueue<String> messageQueue;

    private final AtomicBoolean streamStarted = new AtomicBoolean(false);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        messageQueue = new LinkedBlockingQueue<>(context.getProperty(QUEUE_SIZE).asInteger());
        streamStarted.set(false);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        startTweetStreamService(context);

        final String firstTweet = messageQueue.poll();
        if (firstTweet == null) {
            context.yield();
            return;
        }

        final AtomicInteger tweetCount = new AtomicInteger(1);
        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, out -> {
            final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
            String tweet = firstTweet;
            out.write('[');
            out.write(tweet.getBytes(StandardCharsets.UTF_8));
            while (tweetCount.get() < batchSize && (tweet = messageQueue.poll()) != null) {
                out.write(',');
                out.write(tweet.getBytes(StandardCharsets.UTF_8));
                tweetCount.getAndIncrement();
            }
            out.write(']');
        });

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        attributes.put(CoreAttributes.FILENAME.key(), String.format("%s.json", UUID.randomUUID()));
        attributes.put("tweets", Integer.toString(tweetCount.get()));
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.transfer(flowFile, REL_SUCCESS);

        final String endpointName = context.getProperty(ENDPOINT).getValue();
        final String transitUri = tweetStreamService.getTransitUri(endpointName);

        session.getProvenanceReporter().receive(flowFile, transitUri);
    }

    @OnPrimaryNodeStateChange
    public void onPrimaryNodeStateChange(final PrimaryNodeState newState) {
        if (newState == PrimaryNodeState.PRIMARY_NODE_REVOKED) {
            stopTweetStreamService();
        }
    }

    @OnStopped
    public void onStopped() {
        stopTweetStreamService();
        emptyQueue();
    }

    private void startTweetStreamService(final ProcessContext context) {
        if (streamStarted.compareAndSet(false, true)) {
            tweetStreamService = new TweetStreamService(context, messageQueue, getLogger());
            tweetStreamService.start();
        }

    }

    private void stopTweetStreamService() {
        if (streamStarted.compareAndSet(true, false)) {
            if (tweetStreamService != null) {
                tweetStreamService.stop();
            }
            tweetStreamService = null;

            if (!messageQueue.isEmpty()) {
                getLogger().warn("Stopped consuming stream: unprocessed messages [{}]", messageQueue.size());
            }
        }
    }


    private void emptyQueue() {
        while (!messageQueue.isEmpty()) {
            messageQueue.poll();
        }
    }
}
