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

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.Location.Coordinate;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesFirehoseEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
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

@SupportsBatching
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"twitter", "tweets", "social media", "status", "json"})
@CapabilityDescription("Pulls status changes from Twitter's streaming API. In versions starting with 1.9.0, the Consumer Key and Access Token are marked as sensitive according to https://developer.twitter.com/en/docs/basics/authentication/guides/securing-keys-and-tokens")
@WritesAttribute(attribute = "mime.type", description = "Sets mime type to application/json")
public class GetTwitter extends AbstractProcessor {

    static final AllowableValue ENDPOINT_SAMPLE = new AllowableValue("Sample Endpoint", "Sample Endpoint", "The endpoint that provides public data, aka a 'garden hose'");
    static final AllowableValue ENDPOINT_FIREHOSE = new AllowableValue("Firehose Endpoint", "Firehose Endpoint", "The endpoint that provides access to all tweets");
    static final AllowableValue ENDPOINT_FILTER = new AllowableValue("Filter Endpoint", "Filter Endpoint", "Endpoint that allows the stream to be filtered by specific terms or User IDs");

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor.Builder()
            .name("Twitter Endpoint")
            .description("Specifies which endpoint data should be pulled from")
            .required(true)
            .allowableValues(ENDPOINT_SAMPLE, ENDPOINT_FIREHOSE, ENDPOINT_FILTER)
            .defaultValue(ENDPOINT_SAMPLE.getValue())
            .build();
    public static final PropertyDescriptor CONSUMER_KEY = new PropertyDescriptor.Builder()
            .name("Consumer Key")
            .description("The Consumer Key provided by Twitter")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CONSUMER_SECRET = new PropertyDescriptor.Builder()
            .name("Consumer Secret")
            .description("The Consumer Secret provided by Twitter")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Access Token")
            .description("The Access Token provided by Twitter")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ACCESS_TOKEN_SECRET = new PropertyDescriptor.Builder()
            .name("Access Token Secret")
            .description("The Access Token Secret provided by Twitter")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor LANGUAGES = new PropertyDescriptor.Builder()
            .name("Languages")
            .description("A comma-separated list of languages for which tweets should be fetched")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor FOLLOWING = new PropertyDescriptor.Builder()
            .name("IDs to Follow")
            .description("A comma-separated list of Twitter User ID's to follow. Ignored unless Endpoint is set to 'Filter Endpoint'.")
            .required(false)
            .addValidator(new FollowingValidator())
            .build();
    public static final PropertyDescriptor LOCATIONS = new PropertyDescriptor.Builder()
            .name("Locations to Filter On")
            .description("A comma-separated list of coordinates specifying one or more bounding boxes to filter on."
                    + "Each bounding box is specified by a pair of coordinates in the format: swLon,swLat,neLon,neLat. "
                    + "Multiple bounding boxes can be specified as such: swLon1,swLat1,neLon1,neLat1,swLon2,swLat2,neLon2,neLat2."
                    + "Ignored unless Endpoint is set to 'Filter Endpoint'.")
            .addValidator(new LocationValidator() )
            .required(false)
            .build();
    public static final PropertyDescriptor TERMS = new PropertyDescriptor.Builder()
            .name("Terms to Filter On")
            .description("A comma-separated list of terms to filter on. Ignored unless Endpoint is set to 'Filter Endpoint'."
                    + " The filter works such that if any term matches, the status update will be retrieved; multiple terms"
                    + " separated by a space function as an 'AND'. I.e., 'it was, hello' will retrieve status updates that"
                    + " have either 'hello' or both 'it' AND 'was'")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All status updates will be routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(1000);

    private volatile Client client;
    private volatile BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(5000);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ENDPOINT);
        descriptors.add(CONSUMER_KEY);
        descriptors.add(CONSUMER_SECRET);
        descriptors.add(ACCESS_TOKEN);
        descriptors.add(ACCESS_TOKEN_SECRET);
        descriptors.add(LANGUAGES);
        descriptors.add(TERMS);
        descriptors.add(FOLLOWING);
        descriptors.add(LOCATIONS);
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
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Adds a query parameter with name '" + propertyDescriptorName + "' to the Twitter query")
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>();
        final String endpointName = validationContext.getProperty(ENDPOINT).getValue();

        if (ENDPOINT_FILTER.getValue().equals(endpointName)) {
            if (!validationContext.getProperty(TERMS).isSet() && !validationContext.getProperty(FOLLOWING).isSet() && !validationContext.getProperty(LOCATIONS).isSet()) {
                results.add(new ValidationResult.Builder().input("").subject(FOLLOWING.getName())
                        .valid(false).explanation("When using the 'Filter Endpoint', at least one of '" + TERMS.getName() + "' or '" + FOLLOWING.getName() + "'" +
                                "' or '" + LOCATIONS.getName() + " must be set").build());
            }
        }

        return results;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        // if any property is modified, the results are no longer valid. Destroy all messages in the queue.
        messageQueue.clear();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String endpointName = context.getProperty(ENDPOINT).getValue();
        final Authentication oauth = new OAuth1(context.getProperty(CONSUMER_KEY).getValue(),
                context.getProperty(CONSUMER_SECRET).getValue(),
                context.getProperty(ACCESS_TOKEN).getValue(),
                context.getProperty(ACCESS_TOKEN_SECRET).getValue());

        final ClientBuilder clientBuilder = new ClientBuilder();
        clientBuilder.name("GetTwitter[id=" + getIdentifier() + "]")
                .authentication(oauth)
                .eventMessageQueue(eventQueue)
                .processor(new StringDelimitedProcessor(messageQueue));

        final String languageString = context.getProperty(LANGUAGES).getValue();
        final List<String> languages;
        if (languageString == null) {
            languages = null;
        } else {
            languages = new ArrayList<>();
            for (final String language : context.getProperty(LANGUAGES).getValue().split(",")) {
                languages.add(language.trim());
            }
        }

        final String host;
        final StreamingEndpoint streamingEndpoint;
        if (ENDPOINT_SAMPLE.getValue().equals(endpointName)) {
            host = Constants.STREAM_HOST;
            final StatusesSampleEndpoint sse = new StatusesSampleEndpoint();
            streamingEndpoint = sse;
            if (languages != null) {
                sse.languages(languages);
            }
        } else if (ENDPOINT_FIREHOSE.getValue().equals(endpointName)) {
            host = Constants.STREAM_HOST;
            final StatusesFirehoseEndpoint firehoseEndpoint = new StatusesFirehoseEndpoint();
            streamingEndpoint = firehoseEndpoint;
            if (languages != null) {
                firehoseEndpoint.languages(languages);
            }
        } else if (ENDPOINT_FILTER.getValue().equals(endpointName)) {
            host = Constants.STREAM_HOST;
            final StatusesFilterEndpoint filterEndpoint = new StatusesFilterEndpoint();

            final String followingString = context.getProperty(FOLLOWING).getValue();
            final List<Long> followingIds;
            if (followingString == null) {
                followingIds = Collections.emptyList();
            } else {
                followingIds = new ArrayList<>();

                for (final String split : followingString.split(",")) {
                    final Long id = Long.parseLong(split.trim());
                    followingIds.add(id);
                }
            }

            final String termString = context.getProperty(TERMS).getValue();
            final List<String> terms;
            if (termString == null) {
                terms = Collections.emptyList();
            } else {
                terms = new ArrayList<>();
                for (final String split : termString.split(",")) {
                    terms.add(split.trim());
                }
            }

            if (!terms.isEmpty()) {
                filterEndpoint.trackTerms(terms);
            }

            if (!followingIds.isEmpty()) {
                filterEndpoint.followings(followingIds);
            }

            if (languages != null) {
                filterEndpoint.languages(languages);
            }

            final String locationString = context.getProperty(LOCATIONS).getValue();
            final List<Location> locations;
            if (locationString == null) {
                locations = Collections.emptyList();
            } else {
                locations = LocationUtil.parseLocations(locationString);
            }

            if (!locations.isEmpty()) {
                filterEndpoint.locations(locations);
            }

            streamingEndpoint = filterEndpoint ;

        } else {
            throw new AssertionError("Endpoint was invalid value: " + endpointName);
        }

        clientBuilder.hosts(host).endpoint(streamingEndpoint);
        client = clientBuilder.build();
        client.connect();
    }

    @OnStopped
    public void shutdownClient() {
        if (client != null) {
            client.stop();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final Event event = eventQueue.poll();
        if (event != null) {
            switch (event.getEventType()) {
                case STOPPED_BY_ERROR:
                    getLogger().error("Received error {}: {} due to {}. Will not attempt to reconnect", new Object[]{event.getEventType(), event.getMessage(), event.getUnderlyingException()});
                    break;
                case CONNECTION_ERROR:
                case HTTP_ERROR:
                    getLogger().error("Received error {}: {}. Will attempt to reconnect", new Object[]{event.getEventType(), event.getMessage()});
                    client.reconnect();
                    break;
                default:
                    break;
            }
        }

        final String tweet = messageQueue.poll();
        if (tweet == null) {
            context.yield();
            return;
        }

        FlowFile flowFile = session.create();
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                out.write(tweet.getBytes(StandardCharsets.UTF_8));
            }
        });

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json");
        flowFile = session.putAllAttributes(flowFile, attributes);

        session.transfer(flowFile, REL_SUCCESS);
        session.getProvenanceReporter().receive(flowFile, Constants.STREAM_HOST + client.getEndpoint().getURI());
    }

    private static class FollowingValidator implements Validator {

        private static final Pattern NUMBER_PATTERN = Pattern.compile("\\d+");

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final String[] splits = input.split(",");
            for (final String split : splits) {
                if (!NUMBER_PATTERN.matcher(split.trim()).matches()) {
                    return new ValidationResult.Builder().input(input).subject(subject).valid(false).explanation("Must be comma-separted list of User ID's").build();
                }
            }

            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }

    }

    private static class LocationValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            try {
                final List<Location> locations = LocationUtil.parseLocations(input);
                for (final Location location : locations) {
                    final Coordinate sw = location.southwestCoordinate();
                    final Coordinate ne = location.northeastCoordinate();

                    if (sw.longitude() > ne.longitude()) {
                        return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                                .explanation("SW Longitude (" + sw.longitude() + ") must be less than NE Longitude ("
                                        + ne.longitude() + ").").build();
                    }

                    if (sw.longitude() == ne.longitude()) {
                        return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                                .explanation("SW Longitude (" + sw.longitude() + ") can not be equal to NE Longitude ("
                                        + ne.longitude() + ").").build();
                    }

                    if (sw.latitude() > ne.latitude()) {
                        return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                                .explanation("SW Latitude (" + sw.latitude() + ") must be less than NE Latitude ("
                                        + ne.latitude() + ").").build();
                    }

                    if (sw.latitude() == ne.latitude()) {
                        return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                                .explanation("SW Latitude (" + sw.latitude() + ") can not be equal to NE Latitude ("
                                        + ne.latitude() + ").").build();
                    }
                }

                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();

            } catch (IllegalStateException e) {
                return new ValidationResult.Builder()
                        .input(input).subject(subject).valid(false)
                        .explanation("Must be a comma-separated list of longitude,latitude pairs specifying one or more bounding boxes.")
                        .build();
            }
        }

    }

}