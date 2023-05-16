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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;

import java.util.List;

/**
 * {@link FlowFile} along with associated context metadata, relating to GCP publish via <code>PublishGCPubSub</code> processor.
 */
public class FlowFileResult {
    private final FlowFile flowFile;
    private final List<ApiFuture<String>> futures;
    private Exception exception;
    private Relationship relationship;

    public FlowFileResult(final FlowFile flowFile, final List<ApiFuture<String>> futures) {
        this(flowFile, futures, null);
    }

    public FlowFileResult(final FlowFile flowFile, final List<ApiFuture<String>> futures, final Exception exception) {
        this.flowFile = flowFile;
        this.futures = futures;
        this.exception = exception;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public List<ApiFuture<String>> getFutures() {
        return futures;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Relationship getRelationship() {
        return relationship;
    }

    public void setRelationship(final Relationship relationship) {
        this.relationship = relationship;
    }
}
