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

import com.google.api.core.ApiFuture;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.gcp.pubsub.AbstractGCPubSubProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Tracking of a publish interaction from NiFi <code>PublishGCPubSub</code> processor to GCP PubSub endpoint.
 */
public class MessageTracker {
    private final Collection<FlowFileResult> flowFileResults;
    private final Collection<ApiFuture<String>> futures;

    public MessageTracker() {
        this.flowFileResults = new ArrayList<>();
        this.futures = new ArrayList<>();
    }

    public void add(final FlowFileResult flowFileResult) {
        flowFileResults.add(flowFileResult);
        futures.addAll(flowFileResult.getFutures());
    }

    public int size() {
        return futures.size();
    }

    public Collection<FlowFileResult> getFlowFileResults() {
        return flowFileResults;
    }
    public Collection<ApiFuture<String>> getFutures() {
        return futures;
    }

    /**
     * After all in-flight messages have publish results, annotate record with appropriate {@link Relationship}.  A
     * <code>FlowFile</code> publish is considered to be successful if all associated {@link ApiFuture} are successful,
     * as indicated by {@link com.google.api.core.ApiFutures#successfulAsList(Iterable)}.
     */
    public void reconcile(final List<String> messageIdsSuccess) throws ExecutionException, InterruptedException {
        for (final FlowFileResult flowFileResult : flowFileResults) {
            Relationship relationship = (flowFileResult.getException() == null)
                    ? AbstractGCPubSubProcessor.REL_SUCCESS
                    : AbstractGCPubSubProcessor.REL_FAILURE;
            if (AbstractGCPubSubProcessor.REL_SUCCESS.equals(relationship)) {
                final List<ApiFuture<String>> futures = flowFileResult.getFutures();
                for (ApiFuture<String> future : futures) {
                    if (!messageIdsSuccess.contains(future.get())) {
                        relationship = AbstractGCPubSubProcessor.REL_FAILURE;
                        flowFileResult.setException(new IOException("Send failure " + future.get()));
                        break;
                    }
                }
            }
            flowFileResult.setRelationship(relationship);
        }
    }
}
