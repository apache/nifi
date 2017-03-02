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
package org.apache.nifi.cluster.manager.testutils;

/**
 * Wraps a HttpResponse with a time-delay. When the action is applied, the currently executing thread sleeps for the given delay before returning the response to the caller.
 *
 * This class is good for simulating network latency.
 *
 */
public class HttpResponseAction {

    private final HttpResponse response;

    private final int waitTimeMs;

    public HttpResponseAction(final HttpResponse response) {
        this(response, 0);
    }

    public HttpResponseAction(final HttpResponse response, final int waitTimeMs) {
        this.response = response;
        this.waitTimeMs = waitTimeMs;
    }

    public HttpResponse apply() {
        try {
            Thread.sleep(waitTimeMs);
        } catch (final InterruptedException ie) {
            throw new RuntimeException(ie);
        }

        return response;
    }

    public HttpResponse getResponse() {
        return response;
    }

    public int getWaitTimeMs() {
        return waitTimeMs;
    }
}
