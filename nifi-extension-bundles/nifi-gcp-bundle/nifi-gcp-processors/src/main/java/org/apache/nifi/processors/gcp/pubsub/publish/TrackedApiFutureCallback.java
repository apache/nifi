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
package org.apache.nifi.processors.gcp.pubsub.publish;

import com.google.api.core.ApiFutureCallback;

import java.util.List;

/**
 * Specialization of {@link ApiFutureCallback} used to track Google PubSub send results.  Failure
 * exceptions are captured to facilitate FlowFile routing decisions.
 */
public class TrackedApiFutureCallback implements ApiFutureCallback<String> {
    private final List<String> successes;
    private final List<Throwable> failures;

    public TrackedApiFutureCallback(final List<String> successes, final List<Throwable> failures) {
        this.successes = successes;
        this.failures = failures;
    }

    @Override
    public void onFailure(final Throwable t) {
        failures.add(t);
    }

    @Override
    public void onSuccess(final String result) {
        successes.add(result);
    }
}
