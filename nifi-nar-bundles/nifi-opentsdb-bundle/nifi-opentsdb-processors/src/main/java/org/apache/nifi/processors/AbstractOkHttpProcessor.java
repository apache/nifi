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
package org.apache.nifi.processors;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Description:A base class for all processors which use OkHttp
 *
 * @author bright
 */
public abstract class AbstractOkHttpProcessor extends AbstractProcessor {

    protected static final PropertyDescriptor HTTP_URL = new PropertyDescriptor.Builder()
            .name("http-url")
            .displayName("HTTP URL")
            .description("HTTP URL which will be connected to, including scheme (http, e.g.), host, and port. " +
                    "The default port for the http server is 4242.")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    protected static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("http-connect-timeout")
            .displayName("Connection Timeout")
            .description("Max wait time for the connection to the http server.")
            .required(true)
            .defaultValue("5 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    protected static final PropertyDescriptor RESPONSE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("http-response-timeout")
            .displayName("Response Timeout")
            .description("Max wait time for a response from the http server.")
            .required(true)
            .defaultValue("15 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    protected final AtomicReference<OkHttpClient> okHttpClientAtomicReference = new AtomicReference<>();

    protected void createOkHttpClient(ProcessContext context) throws ProcessException {
        okHttpClientAtomicReference.set(null);

        OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();

        // Set timeouts
        okHttpClient.connectTimeout((context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue()), TimeUnit.MILLISECONDS);
        okHttpClient.readTimeout(context.getProperty(RESPONSE_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue(), TimeUnit.MILLISECONDS);

        okHttpClientAtomicReference.set(okHttpClient.build());
    }

    protected Response getResponse(URL url, String verb, RequestBody body) throws Exception {
        final ComponentLog logger = getLogger();

        Request.Builder requestBuilder = new Request.Builder().url(url);

        if ("get".equalsIgnoreCase(verb)) {
            requestBuilder = requestBuilder.get();
        } else if ("put".equalsIgnoreCase(verb)) {
            requestBuilder = requestBuilder.put(body);
        } else if ("post".equalsIgnoreCase(verb)) {
            requestBuilder = requestBuilder.post(body);
        } else {
            throw new IllegalArgumentException("Request verb not supported by this processor: " + verb);
        }

        Request httpRequest = requestBuilder.build();

        logger.debug("Send request to {}", new Object[]{url});

        Response response = okHttpClientAtomicReference.get().newCall(httpRequest).execute();

        int statusCode = response.code();

        if (statusCode == 0) {
            throw new IllegalStateException("Status code unknown, connection hasn't been attempted.");
        }

        logger.debug("Received response from http server with status code {}", new Object[]{statusCode});

        return response;
    }
}
