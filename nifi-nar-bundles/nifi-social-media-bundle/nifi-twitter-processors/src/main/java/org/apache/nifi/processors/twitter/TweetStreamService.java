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

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TweetStreamService {
    public static final String SEARCH_ENDPOINT = "Search Endpoint";
    public static final String SAMPLE_ENDPOINT = "Sample Endpoint";
    private static final String SAMPLE_PATH = "/2/tweets/sample/stream";
    private static final String SEARCH_PATH = "/2/tweets/search/stream";

    private static final String BEARER_TOKEN_PROPERTY_NAME = "bearer-token";
    private static final String TWEET_FIELDS_PROPERTY_NAME = "tweet-fields";
    private static final String USER_FIELDS_PROPERTY_NAME = "user-fields";
    private static final String MEDIA_FIELDS_PROPERTY_NAME = "media-fields";
    private static final String POLL_FIELDS_PROPERTY_NAME = "poll-fields";
    private static final String PLACE_FIELDS_PROPERTY_NAME = "place-fields";
    private static final String EXPANSIONS_PROPERTY_NAME = "expansions";
    private static final String BACKFILL_MINUTES_PROPERTY_NAME = "backfill-minutes";

    private final BlockingQueue<String> queue;
    private final ComponentLog logger;

    private final ExecutorService executorService;

    private final Set<String> tweetFields;
    private final Set<String> userFields;
    private final Set<String> mediaFields;
    private final Set<String> pollFields;
    private final Set<String> placeFields;
    private final Set<String> expansions;
    private final int backfillMinutes;
    private final TwitterApi api;
    private InputStream stream;

    private Set<String> parseCommaSeparatedListPropreties(final ProcessContext context, final String propertyName) {
        Set<String> fields = null;
        if (context.getProperty(propertyName).isSet()) {
            fields = new HashSet<>();
            final String fieldsString = context.getProperty(propertyName).getValue();
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

        this.tweetFields = parseCommaSeparatedListPropreties(context, TWEET_FIELDS_PROPERTY_NAME);
        this.userFields = parseCommaSeparatedListPropreties(context, USER_FIELDS_PROPERTY_NAME);
        this.mediaFields = parseCommaSeparatedListPropreties(context, MEDIA_FIELDS_PROPERTY_NAME);
        this.pollFields = parseCommaSeparatedListPropreties(context, POLL_FIELDS_PROPERTY_NAME);
        this.placeFields = parseCommaSeparatedListPropreties(context, PLACE_FIELDS_PROPERTY_NAME);
        this.expansions = parseCommaSeparatedListPropreties(context, EXPANSIONS_PROPERTY_NAME);
        this.backfillMinutes = context.getProperty(BACKFILL_MINUTES_PROPERTY_NAME).asInteger();

        TwitterCredentialsBearer creds = new TwitterCredentialsBearer(context.getProperty(BEARER_TOKEN_PROPERTY_NAME).getValue());
        api = new TwitterApi();
        api.setTwitterCredentials(creds);

        this.executorService = Executors.newSingleThreadExecutor();
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
        try {
            if (endpoint.equals(SAMPLE_ENDPOINT)) {
                stream = api.tweets().sampleStream(expansions, tweetFields, userFields, mediaFields, placeFields, pollFields, backfillMinutes);
            } else {
                stream = api.tweets().searchStream(expansions, tweetFields, userFields, mediaFields, placeFields, pollFields, backfillMinutes);
            }
        } catch (ApiException e) {
            throw new ProcessException(String.format("Received error %d: %s", e.getCode(), e.getMessage()), e);
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        if (stream == null) {
            throw new ProcessException("Stream is null, could not make a connection to the Twitter API");
        }

        executorService.execute(new TweetStreamHandler());
    }

    /**
     * This method would be called when we would like the stream to get stopped. The stream will be closed and the
     * executorService will be shut down. If it fails to shutdown, then it will be forcefully terminated.
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

    private class TweetStreamHandler implements Runnable {
        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
                String tweetRecord = reader.readLine();
                while (tweetRecord != null) {
                    queue.offer(tweetRecord);
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
        }
    }

}