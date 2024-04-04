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

import com.twitter.clientlib.ApiClient;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class TweetStreamService {
    private final BlockingQueue<String> queue;
    private final ComponentLog logger;

    private ScheduledExecutorService executorService;
    private final ThreadFactory threadFactory;

    private final Set<String> tweetFields;
    private final Set<String> userFields;
    private final Set<String> mediaFields;
    private final Set<String> pollFields;
    private final Set<String> placeFields;
    private final Set<String> expansions;
    private final int backfillMinutes;
    private final TwitterApi api;
    private InputStream stream;

    private final int backoffAttempts;
    private final long backoffTime;
    private final long maximumBackoff;

    private long backoffMultiplier;
    private int attemptCounter;

    private final StreamEndpoint endpoint;

    public TweetStreamService(final ProcessContext context, final BlockingQueue<String> queue, final ComponentLog logger) {
        Objects.requireNonNull(context);
        Objects.requireNonNull(queue);
        Objects.requireNonNull(logger);

        this.queue = queue;
        this.logger = logger;

        final String endpointName = context.getProperty(ConsumeTwitter.ENDPOINT).getValue();
        if (ConsumeTwitter.ENDPOINT_SAMPLE.getValue().equals(endpointName)) {
            this.endpoint = StreamEndpoint.SAMPLE_ENDPOINT;
        } else {
            this.endpoint = StreamEndpoint.SEARCH_ENDPOINT;
        }

        this.tweetFields = parseCommaSeparatedProperties(context, ConsumeTwitter.TWEET_FIELDS);
        this.userFields = parseCommaSeparatedProperties(context, ConsumeTwitter.USER_FIELDS);
        this.mediaFields = parseCommaSeparatedProperties(context, ConsumeTwitter.MEDIA_FIELDS);
        this.pollFields = parseCommaSeparatedProperties(context, ConsumeTwitter.POLL_FIELDS);
        this.placeFields = parseCommaSeparatedProperties(context, ConsumeTwitter.PLACE_FIELDS);
        this.expansions = parseCommaSeparatedProperties(context, ConsumeTwitter.EXPANSIONS);
        this.backfillMinutes = context.getProperty(ConsumeTwitter.BACKFILL_MINUTES).asInteger();

        this.backoffMultiplier = 1L;
        this.backoffAttempts = context.getProperty(ConsumeTwitter.BACKOFF_ATTEMPTS).asInteger();
        this.attemptCounter = 0;
        this.backoffTime = context.getProperty(ConsumeTwitter.BACKOFF_TIME).asTimePeriod(TimeUnit.SECONDS);
        this.maximumBackoff = context.getProperty(ConsumeTwitter.MAXIMUM_BACKOFF_TIME).asTimePeriod(TimeUnit.SECONDS);

        ApiClient client = new ApiClient();
        final int connectTimeout = context.getProperty(ConsumeTwitter.CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final int readTimeout = context.getProperty(ConsumeTwitter.READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final TwitterCredentialsBearer bearer = new TwitterCredentialsBearer(context.getProperty(ConsumeTwitter.BEARER_TOKEN).getValue());
        client.setConnectTimeout(connectTimeout);
        client.setReadTimeout(readTimeout);
        client.setTwitterCredentials(bearer);
        api = new TwitterApi(client);


        final String basePath = context.getProperty(ConsumeTwitter.BASE_PATH).getValue();
        api.getApiClient().setBasePath(basePath);

        threadFactory = new BasicThreadFactory.Builder().namingPattern(ConsumeTwitter.class.getSimpleName()).build();
    }

    public String getTransitUri(final String endpoint) {
        if (endpoint.equals(StreamEndpoint.SAMPLE_ENDPOINT.getEndpointName())) {
            return api.getApiClient().getBasePath() + StreamEndpoint.SAMPLE_ENDPOINT.getPath();
        } else if (endpoint.equals(StreamEndpoint.SEARCH_ENDPOINT.getEndpointName())) {
            return api.getApiClient().getBasePath() + StreamEndpoint.SEARCH_ENDPOINT.getPath();
        } else {
            logger.warn("Unrecognized endpoint in getTransitUri. Returning basePath");
            return api.getApiClient().getBasePath();
        }
    }

    /**
     * This method would be called when we would like the stream to get started. This method will spin off a thread that
     * will continue to queue tweets on to the given queue passed in the constructor. The thread will continue
     * to run until {@code stop} is called.
     */
    public void start() {
        this.executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        executorService.execute(new TweetStreamStarter());
    }

    /**
     * This method would be called when we would like the stream to get stopped. The stream will be closed and the
     * executorService will be shut down.
     */
    public void stop() {
        if (stream != null) {
            try {
                stream.close();
            } catch (IOException e) {
                logger.error("Closing response stream failed", e);
            }
        }

        executorService.shutdownNow();
        executorService = null;
    }

    private Long calculateBackoffDelay() {
        long backoff = backoffMultiplier * backoffTime;
        return Math.min(backoff, maximumBackoff);
    }

    private void scheduleStartStreamWithBackoff() {
        // use exponential(by factor of 2) backoff in scheduling the next TweetStreamStarter
        if (attemptCounter >= backoffAttempts) {
            throw new ProcessException(String.format("Connection failed after maximum attempts [%d]", attemptCounter));
        }
        attemptCounter += 1;
        long delay = calculateBackoffDelay();
        backoffMultiplier *= 2;
        logger.info("Scheduling new stream connection after delay [{} s]", delay);
        executorService.schedule(new TweetStreamStarter(), delay, TimeUnit.SECONDS);
    }

    private void resetBackoff() {
        attemptCounter = 0;
        backoffMultiplier = 1L;
    }

    private class TweetStreamStarter implements Runnable {
        @Override
        public void run() {
            try {
                if (endpoint.equals(StreamEndpoint.SAMPLE_ENDPOINT)) {
                    stream = api.tweets().sampleStream()
                            .expansions(expansions)
                            .tweetFields(tweetFields)
                            .userFields(userFields)
                            .mediaFields(mediaFields)
                            .placeFields(placeFields)
                            .pollFields(pollFields)
                            .backfillMinutes(backfillMinutes)
                            .execute();
                } else {
                    stream = api.tweets().searchStream()
                            .expansions(expansions)
                            .tweetFields(tweetFields)
                            .userFields(userFields)
                            .mediaFields(mediaFields)
                            .placeFields(placeFields)
                            .pollFields(pollFields)
                            .backfillMinutes(backfillMinutes)
                            .execute();
                }
                executorService.execute(new TweetStreamHandler());
            } catch (final ApiException e) {
                stream = null;
                logger.warn("Twitter Stream [{}] API connection failed: HTTP {}", endpoint.getEndpointName(), e.getCode(), e);
                scheduleStartStreamWithBackoff();
            } catch (final Exception e) {
                stream = null;
                logger.warn("Twitter Stream [{}] connection failed", endpoint.getEndpointName(), e);
                scheduleStartStreamWithBackoff();
            }
        }
    }

    private class TweetStreamHandler implements Runnable {
        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String tweetRecord = reader.readLine();
                while (tweetRecord != null) {
                    // Skip empty lines received from the Twitter Stream
                    if (tweetRecord.isEmpty()) {
                        tweetRecord = reader.readLine();
                        continue;
                    }

                    queue.put(tweetRecord);

                    // reset backoff multiplier upon successful receipt of a tweet
                    resetBackoff();

                    tweetRecord = reader.readLine();
                }
            } catch (final IOException e) {
                logger.info("Stream is closed or has stopped", e);
            } catch (final InterruptedException e) {
                logger.info("Interrupted while adding Tweet to queue", e);
                return;
            }
            logger.info("Stream processing completed");
            scheduleStartStreamWithBackoff();
        }
    }

    private Set<String> parseCommaSeparatedProperties(final ProcessContext context, final PropertyDescriptor property) {
        Set<String> fields = null;
        if (context.getProperty(property).isSet()) {
            fields = new HashSet<>();
            final String fieldsString = context.getProperty(property).getValue();
            for (final String field: fieldsString.split(",")) {
                fields.add(field.trim());
            }
        }
        return fields;
    }
}
