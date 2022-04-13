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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@PrimaryNodeOnly
@SupportsBatching
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"twitter", "tweets", "social media", "status", "json"})
@CapabilityDescription("Streams tweets from Twitter's streaming API v2. The stream provides a sample stream or a search "
    + "stream based on previously uploaded rules. This processor also provides a pass through for certain fields of the "
    + "tweet to be returned as part of the response. See "
    + "https://developer.twitter.com/en/docs/twitter-api/data-dictionary/introduction for more information regarding the "
    + "Tweet object model. \n\n"
    + "Warning: the underlying Java SDK used is still in beta as of the publishing of this processor feature.")
@WritesAttribute(attribute = "mime.type", description = "Sets mime type to application/json")
public class ConsumeTwitter extends AbstractProcessor {

    static final AllowableValue ENDPOINT_SAMPLE = new AllowableValue("Sample Endpoint",
            "Sample Endpoint",
            "The endpoint that provides a stream of about 1% of tweets in real-time");
    static final AllowableValue ENDPOINT_SEARCH = new AllowableValue("Search Endpoint",
            "Search Endpoint",
            "The endpoint that provides a stream of tweets that matches the rules you added to the stream. " +
                    "If rules are not configured, then the stream will be empty");

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor.Builder()
            .name("consume-twitter-endpoint")
            .displayName("Twitter Endpoint")
            .description("Specifies which endpoint tweets should be pulled from. " +
                    "Usage of search endpoint requires that rules be uploaded beforehand. See " +
                    "https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/api-reference/" +
                    "post-tweets-search-stream-rules")
            .required(true)
            .allowableValues(ENDPOINT_SAMPLE, ENDPOINT_SEARCH)
            .defaultValue(ENDPOINT_SAMPLE.getValue())
            .build();
    public static final PropertyDescriptor BASE_PATH = new PropertyDescriptor.Builder()
            .name("base-path")
            .displayName("Base Path")
            .description("Specifies which base path the API client will use for HTTP requests. " +
                    "Generally should not be changed from the default https://api.twitter.com except for testing")
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
            .description("The maximum size of the number of tweets to be written to a single FlowFile." +
                    "Will write less tweets if it there are not any tweets left in queue")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
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

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing an array of one or more Tweets")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private TweetStreamService tweetStreamService;

    private volatile BlockingQueue<String> messageQueue;

    @Override
    protected void init(ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ENDPOINT);
        descriptors.add(BASE_PATH);
        descriptors.add(BEARER_TOKEN);
        descriptors.add(QUEUE_SIZE);
        descriptors.add(BATCH_SIZE);
        descriptors.add(TWEET_FIELDS);
        descriptors.add(USER_FIELDS);
        descriptors.add(MEDIA_FIELDS);
        descriptors.add(POLL_FIELDS);
        descriptors.add(PLACE_FIELDS);
        descriptors.add(EXPANSIONS);
        descriptors.add(BACKFILL_MINUTES);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        messageQueue = new LinkedBlockingQueue<>(context.getProperty(QUEUE_SIZE).asInteger());

        tweetStreamService = new TweetStreamService(context, messageQueue, getLogger());
        tweetStreamService.setBasePath(context.getProperty(BASE_PATH).getValue());
        final String endpointName = context.getProperty(ENDPOINT).getValue();
        if (ENDPOINT_SAMPLE.getValue().equals(endpointName)) {
            tweetStreamService.start(TweetStreamService.SAMPLE_ENDPOINT);
        } else {
            tweetStreamService.start(TweetStreamService.SEARCH_ENDPOINT);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        if (messageQueue.isEmpty()) {
            context.yield();
            return;
        }

        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write('[');
                final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
                String tweet = messageQueue.poll();
                out.write(tweet.getBytes(StandardCharsets.UTF_8));
                int tweetCount = 1;
                while (tweetCount < batchSize && (tweet = messageQueue.poll()) != null) {
                    out.write(',');
                    out.write(tweet.getBytes(StandardCharsets.UTF_8));
                    tweetCount++;
                }
                out.write(']');
            }
        });

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        attributes.put(CoreAttributes.FILENAME.key(), String.format("%s.json", UUID.randomUUID()));
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.transfer(flowFile, REL_SUCCESS);

        final String endpointName = context.getProperty(ENDPOINT).getValue();
        final String transitUri = tweetStreamService.getTransitUri(endpointName);

        session.getProvenanceReporter().receive(flowFile, transitUri);
    }

    @OnStopped
    public void onStopped() {
        if (tweetStreamService != null) {
            tweetStreamService.stop();
        }
        tweetStreamService = null;
        emptyQueue();
    }

    private void emptyQueue() {
        while (!messageQueue.isEmpty()) {
            messageQueue.poll();
        }
    }
}
