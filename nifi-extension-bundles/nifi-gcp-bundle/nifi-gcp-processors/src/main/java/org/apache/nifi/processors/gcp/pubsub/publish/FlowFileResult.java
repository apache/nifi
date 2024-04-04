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
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.gcp.pubsub.PubSubAttributes;
import org.apache.nifi.processors.gcp.pubsub.PublishGCPubSub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Tracking of an interaction from NiFi {@link PublishGCPubSub} processor to Google PubSub endpoint.
 */
public class FlowFileResult {
    private static final Logger logger = LoggerFactory.getLogger(FlowFileResult.class);

    private final FlowFile flowFile;
    private final Map<String, String> attributes;
    private final List<ApiFuture<String>> futures;
    private final List<String> successes;
    private final List<Throwable> failures;

    public FlowFileResult(final FlowFile flowFile, final List<ApiFuture<String>> futures,
                          final List<String> successes, final List<Throwable> failures) {
        this.flowFile = flowFile;
        this.attributes = new LinkedHashMap<>();
        this.futures = futures;
        this.successes = successes;
        this.failures = failures;
    }

    /**
     * After all in-flight messages have results, calculate appropriate {@link Relationship}.
     */
    public Relationship reconcile() {
        while (futures.size() > (successes.size() + failures.size())) {
            try {
                ApiFutures.allAsList(futures).get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Failed to reconcile PubSub send operation status", e);
            }
        }
        if (futures.size() == successes.size()) {
            if (futures.size() == 1) {
                attributes.put(PubSubAttributes.MESSAGE_ID_ATTRIBUTE, successes.iterator().next());
            } else {
                attributes.put(PubSubAttributes.RECORDS_ATTRIBUTE, Integer.toString(futures.size()));
            }
        }
        return RelationshipMapper.toRelationship(failures);
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    /**
     * Logic to derive an appropriate {@link Relationship} from the feedback provided by the client library.
     * <p>
     * Each {@link com.google.pubsub.v1.PubsubMessage} is associated with a {@link TrackedApiFutureCallback} at time of
     * submission to the client library.  This callback allows the client library to convey information to the caller
     * about the result of the (asynchronous) send.  If a send fails, an appropriate exception is conveyed, providing
     * detail about the send failure; otherwise a message id (provided by the service) is supplied.
     * <p>
     * Types of exceptions might be classified into "retryable" (another send may be attempted) or non-retryable.
     */
    private static class RelationshipMapper {

        private static Relationship toRelationship(final List<Throwable> failures) {
            Relationship relationship = PublishGCPubSub.REL_SUCCESS;
            boolean isRetry = false;
            boolean isFailure = false;
            for (final Throwable failure : failures) {
                if (isRetryException(failure)) {
                    isRetry = true;
                } else {
                    isFailure = true;
                    break;
                }
            }
            if (isFailure) {
                relationship = PublishGCPubSub.REL_FAILURE;
            } else if (isRetry) {
                relationship = PublishGCPubSub.REL_RETRY;
            }
            return relationship;
        }

        /**
         * Retryable exceptions indicate transient conditions; another send attempt might succeed.
         */
        private static final Collection<Class<? extends Throwable>> RETRY_EXCEPTIONS = Collections.singleton(
                UnavailableException.class);

        /**
         * Exceptions provided by client library might include a nested exception that indicates a transient condition,
         * so the entire exception chain should be checked.
         */
        private static boolean isRetryException(final Throwable t) {
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
    }
}
