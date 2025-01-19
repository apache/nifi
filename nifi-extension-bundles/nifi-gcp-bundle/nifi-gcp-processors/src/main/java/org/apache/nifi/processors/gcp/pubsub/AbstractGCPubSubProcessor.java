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
package org.apache.nifi.processors.gcp.pubsub;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;

import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractGCPubSubProcessor extends AbstractGCPProcessor implements VerifiableProcessor {

    public static final PropertyDescriptor BATCH_SIZE_THRESHOLD = new PropertyDescriptor.Builder()
            .name("gcp-pubsub-publish-batch-size")
            .displayName("Batch Size Threshold")
            .description("Indicates the number of messages the cloud service should bundle together in a batch. If not set and left empty, only one message " +
                    "will be used in a batch")
            .required(true)
            .defaultValue("15")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_BYTES_THRESHOLD = new PropertyDescriptor.Builder()
            .name("gcp-batch-bytes")
            .displayName("Batch Bytes Threshold")
            .description("Publish request gets triggered based on this Batch Bytes Threshold property and"
                    + " the " + BATCH_SIZE_THRESHOLD.getDisplayName() + " property, whichever condition is met first.")
            .required(true)
            .defaultValue("3 MB")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_DELAY_THRESHOLD = new PropertyDescriptor.Builder()
            .name("gcp-pubsub-publish-batch-delay")
            .displayName("Batch Delay Threshold")
            .description("Indicates the delay threshold to use for batching. After this amount of time has elapsed " +
                    "(counting from the first element added), the elements will be wrapped up in a batch and sent. " +
                    "This value should not be set too high, usually on the order of milliseconds. Otherwise, calls " +
                    "might appear to never complete.")
            .required(true)
            .defaultValue("100 millis")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor API_ENDPOINT = new PropertyDescriptor
            .Builder().name("api-endpoint")
            .displayName("API Endpoint")
            .description("Override the gRPC endpoint in the form of [host:port]")
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .defaultValue(PublisherStubSettings.getDefaultEndpoint())  // identical to SubscriberStubSettings.getDefaultEndpoint()
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to this relationship after a successful Google Cloud Pub/Sub operation.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship if the Google Cloud Pub/Sub operation fails.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected ServiceOptions getServiceOptions(ProcessContext context, GoogleCredentials credentials) {
        return null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new HashSet<>(super.customValidate(validationContext));

        final boolean projectId = validationContext.getProperty(PROJECT_ID).isSet();
        if (!projectId) {
            results.add(new ValidationResult.Builder()
                    .subject(PROJECT_ID.getName())
                    .valid(false)
                    .explanation("The Project ID must be set for this processor.")
                    .build());
        }

        return results;
    }

}
