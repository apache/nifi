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
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.UnavailableException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.gcp.pubsub.PubSubAttributes;
import org.apache.nifi.processors.gcp.pubsub.PublishGCPubSub;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Tracking of an interaction from NiFi {@link PublishGCPubSub} processor to Google PubSub endpoint.
 */
public class FlowFileResult {
    // Retry-able exceptions indicate transient conditions; another send attempt might succeed.
    private static final Collection<Class<? extends Throwable>> RETRY_EXCEPTIONS =
            Collections.singleton(UnavailableException.class);

    private final FlowFile flowFile;
    private final Map<String, String> attributes;
    private final List<ApiFuture<String>> futures;
    private final Throwable ffError;

    public FlowFileResult(final FlowFile flowFile, final List<ApiFuture<String>> futures, final Throwable ffError) {
        this.flowFile = flowFile;
        this.attributes = new LinkedHashMap<>();
        this.futures = futures;
        this.ffError = ffError;
    }

    /**
     * After all in-flight messages have results, calculate appropriate {@link Relationship}.
     */
    public Relationship reconcile(final ComponentLog componentLog) {
        if (ffError != null) {
            componentLog.error("Cannot publish FlowFile", ffError);
            return PublishGCPubSub.REL_FAILURE;
        }

        try {
            ApiFutures.allAsList(futures).get();
        } catch (InterruptedException | ExecutionException e) {
            componentLog.error("Failed to reconcile PubSub send operation status", e);
            return PublishGCPubSub.REL_RETRY;
        }

        boolean isRetry = false;
        Throwable t = null;
        String messageId = null;
        for (ApiFuture<String> future : futures) {
            try {
                messageId = future.get();
            } catch (CancellationException | InterruptedException e) {
                // We interrupted the operation. Redirect to retry.
                componentLog.error("Operation has been interrupted. Sending to retry", e);
                return PublishGCPubSub.REL_RETRY;
            } catch (ExecutionException e) {
                // the future threw an exception
                t = future.exceptionNow();
                if (isRetryException(t)) {
                    isRetry = true;
                } else {
                    componentLog.error("Operation failed. Sending to failure", t);
                    return PublishGCPubSub.REL_FAILURE;
                }
            }
        }

        if (isRetry) {
            componentLog.error("Operation failed but can be retried. Sending to retry", t);
            return PublishGCPubSub.REL_RETRY;
        }

        if (futures.size() == 1) {
            attributes.put(PubSubAttributes.MESSAGE_ID_ATTRIBUTE, messageId);
        } else {
            attributes.put(PubSubAttributes.RECORDS_ATTRIBUTE, Integer.toString(futures.size()));
        }

        return PublishGCPubSub.REL_SUCCESS;
    }

    /**
     * Exceptions provided by client library might include a nested exception that indicates a transient condition,
     * so the entire exception chain should be checked.
     */
    public static boolean isRetryException(final Throwable t) {
        if (t == null) {
            return false;
        } else if (RETRY_EXCEPTIONS.contains(t.getClass())) {
            return true;
        } else {
            final Throwable cause = t.getCause();
            if (t.equals(cause)) {
                return false;
            } else {
                return isRetryException(cause);
            }
        }
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public Throwable getError() {
        return ffError;
    }

    public List<ApiFuture<String>> getFutures() {
        return futures;
    }
}
