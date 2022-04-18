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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TweetStreamService {
    public static final String SEARCH_ENDPOINT = "Search Endpoint";
    public static final String SAMPLE_ENDPOINT = "Sample Endpoint";
    private static final String SAMPLE_PATH = "/2/tweets/sample/stream";
    private static final String SEARCH_PATH = "/2/tweets/search/stream";

    private final BlockingQueue<String> queue;
    private final ComponentLog logger;

    private final ScheduledExecutorService executorService;

    private final Set<String> tweetFields;
    private final Set<String> userFields;
    private final Set<String> mediaFields;
    private final Set<String> pollFields;
    private final Set<String> placeFields;
    private final Set<String> expansions;
    private final int backfillMinutes;
    private final TwitterApi api;
    private InputStream stream;

    private Long backoffMultiplier;
    private int backoffAttempts;
    private int attemptCounter;
    private Long backoffTime;
    private Long maximumBackoff;

    private String endpoint;

    private Set<String> parseCommaSeparatedListPropreties(final ProcessContext context, final PropertyDescriptor property) {
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

    public TweetStreamService(final ProcessContext context, final BlockingQueue<String> queue, final ComponentLog logger) {
        assert context != null;
        assert queue != null;
        assert logger != null;

        this.queue = queue;
        this.logger = logger;

        this.tweetFields = parseCommaSeparatedListPropreties(context, ConsumeTwitter.TWEET_FIELDS);
        this.userFields = parseCommaSeparatedListPropreties(context, ConsumeTwitter.USER_FIELDS);
        this.mediaFields = parseCommaSeparatedListPropreties(context, ConsumeTwitter.MEDIA_FIELDS);
        this.pollFields = parseCommaSeparatedListPropreties(context, ConsumeTwitter.POLL_FIELDS);
        this.placeFields = parseCommaSeparatedListPropreties(context, ConsumeTwitter.PLACE_FIELDS);
        this.expansions = parseCommaSeparatedListPropreties(context, ConsumeTwitter.EXPANSIONS);
        this.backfillMinutes = context.getProperty(ConsumeTwitter.BACKFILL_MINUTES).asInteger();

        this.backoffMultiplier = 1L;
        this.backoffAttempts = context.getProperty(ConsumeTwitter.BACKOFF_ATTEMPTS).asInteger();
        this.attemptCounter = 0;
        this.backoffTime = context.getProperty(ConsumeTwitter.BACKOFF_TIME).asLong();
        this.maximumBackoff = context.getProperty(ConsumeTwitter.MAXIMUM_BACKOFF_TIME).asLong();

        ApiClient client = new ApiClient();
        client = client.setConnectTimeout(context.getProperty(ConsumeTwitter.CONNECT_TIMEOUT).asInteger());
        api = new TwitterApi(client);

        TwitterCredentialsBearer creds = new TwitterCredentialsBearer(context.getProperty(ConsumeTwitter.BEARER_TOKEN).getValue());
        api.setTwitterCredentials(creds);

        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public String getBasePath() {
        return api.getApiClient().getBasePath();
    }

    public void setBasePath(final String path) {
        api.getApiClient().setBasePath(path);
    }

    public String getTransitUri(final String endpoint) {
        if (endpoint.equals(SAMPLE_ENDPOINT)) {
            return api.getApiClient().getBasePath() + SAMPLE_PATH;
        } else if (endpoint.equals(SEARCH_ENDPOINT)) {
            return api.getApiClient().getBasePath() + SEARCH_PATH;
        } else {
            logger.warn("Unrecognized endpoint in getTransitUri. Returning basePath");
            return api.getApiClient().getBasePath();
        }
    }

    /**
     * This method would be called when we would like the stream to get started. This method will spin off a thread that
     * will continue to queue tweets on to the given queue passed in the constructor. The thread will continue
     * to run until {@code stop} is called.
     * @param endpoint {@code TwitterStreamAPI.SAMPLE_ENDPOINT} or {@code TwitterStreamAPI.SEARCH_ENDPOINT}
     */
    public void start(final String endpoint) {
        this.endpoint = endpoint;
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
    }

    private Long calculateBackoffDelay() {
        long backoff = backoffMultiplier * backoffTime;
        long delay = (backoff > maximumBackoff) ? maximumBackoff : backoff;
        return delay;
    }

    private void scheduleStartStreamWithBackoff() {
        // use exponential(by factor of 2) backoff in scheduling the next TweetStreamStarter
        if (attemptCounter >= backoffAttempts) {
            final String msg = "Reached maximum attempts to backoff";
            logger.error(msg);
            throw new ProcessException(msg);
        }
        attemptCounter += 1;
        long delay = calculateBackoffDelay();
        backoffMultiplier *= 2;
        logger.info("Scheduling new TweetStreamStarter after {}s delay", delay);
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
                if (endpoint.equals(SAMPLE_ENDPOINT)) {
                    stream = api.tweets().sampleStream(expansions, tweetFields, userFields, mediaFields, placeFields, pollFields, backfillMinutes);
                } else {
                    stream = api.tweets().searchStream(expansions, tweetFields, userFields, mediaFields, placeFields, pollFields, backfillMinutes);
                }
            } catch (ApiException e) {
                stream = null;
                logger.warn(String.format("Received ApiException, error %d: %s", e.getCode(), e.getMessage()));
                scheduleStartStreamWithBackoff();
                return;
            } catch (Exception e) {
                stream = null;
                logger.warn(String.format("Received Exception %s: %s", e.getClass(), e.getMessage()));
                scheduleStartStreamWithBackoff();
                return;
            }

            if (stream == null) {
                logger.warn("Stream is null, could not make a connection to the Twitter API");
                scheduleStartStreamWithBackoff();
                return;
            } else {
                executorService.execute(new TweetStreamHandler());
            }
        }
    }

    private class TweetStreamHandler implements Runnable {
        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
                String tweetRecord = reader.readLine();
                while (tweetRecord != null) {
                    queue.offer(tweetRecord);

                    // reset backoff multiplier upon successful receipt of a tweet
                    resetBackoff();
                    try {
                        tweetRecord = reader.readLine();
                    } catch (IOException e) {
                        logger.info("Read Tweet failed: Stream processing completed");
                        break;
                    }
                }
            } catch (IOException e) {
                logger.warn("Stream processing failed", e);
            }
            logger.info("TweetStreamHandler terminating");
            scheduleStartStreamWithBackoff();
        }
    }

}