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
package org.apache.nifi.processors.standard;

import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HandleHttpRequestTest {

    private static final String CONTEXT_MAP_ID = HttpContextMap.class.getSimpleName();

    private static final String MINIMUM_THREADS = "8";

    private static final String URL_FORMAT = "http://127.0.0.1:%d";

    @Mock
    HttpContextMap httpContextMap;

    TestRunner runner;

    HandleHttpRequest handleHttpRequest;

    @BeforeEach
    void setRunner() throws InitializationException {
        handleHttpRequest = new HandleHttpRequest();
        runner = TestRunners.newTestRunner(handleHttpRequest);

        when(httpContextMap.getIdentifier()).thenReturn(CONTEXT_MAP_ID);
        runner.addControllerService(CONTEXT_MAP_ID, httpContextMap);
        runner.enableControllerService(httpContextMap);
    }

    @AfterEach
    void shutdown() {
        runner.shutdown();
    }

    @Test
    void testSetRequiredProperties() {
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);

        runner.assertValid();
    }

    @Test
    void testRun() {
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);
        runner.setProperty(HandleHttpRequest.MAXIMUM_THREADS, MINIMUM_THREADS);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        runner.run();

        runner.assertTransferCount(HandleHttpRequest.REL_SUCCESS, 0);
    }

    @Test
    void testRunMethodNotAllowed() throws InterruptedException {
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);
        runner.setProperty(HandleHttpRequest.MAXIMUM_THREADS, MINIMUM_THREADS);
        runner.setProperty(HandleHttpRequest.PORT, "0");
        runner.setProperty(HandleHttpRequest.ALLOW_GET, Boolean.FALSE.toString());

        runner.run(1, false);
        runner.assertTransferCount(HandleHttpRequest.REL_SUCCESS, 0);

        final AtomicInteger responseCodeHolder = new AtomicInteger();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final Thread requestThread = Thread.ofVirtual().unstarted(() -> {
            try {
                final URL url = getUrl();
                final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.connect();
                final int responseCode = connection.getResponseCode();
                responseCodeHolder.set(responseCode);
                countDownLatch.countDown();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        });
        requestThread.start();

        final boolean completed = countDownLatch.await(5, TimeUnit.SECONDS);
        assertTrue(completed, "HTTP request failed");
        assertEquals(HttpURLConnection.HTTP_BAD_METHOD, responseCodeHolder.get());

        runner.run(1, true, false);
    }

    private URL getUrl() {
        final int port = handleHttpRequest.getPort();
        final URI uri = URI.create(URL_FORMAT.formatted(port));
        try {
            return uri.toURL();
        } catch (final MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
