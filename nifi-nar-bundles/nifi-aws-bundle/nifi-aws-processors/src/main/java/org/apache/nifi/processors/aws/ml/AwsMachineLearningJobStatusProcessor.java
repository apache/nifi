/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.processors.aws.ml;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

public abstract class AwsMachineLearningJobStatusProcessor<T extends AmazonWebServiceClient>
        extends AbstractAWSCredentialsProviderProcessor<T>  {
    public static final String AWS_TASK_OUTPUT_LOCATION = "outputLocation";
    public static final PropertyDescriptor MANDATORY_AWS_CREDENTIALS_PROVIDER_SERVICE =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(AWS_CREDENTIALS_PROVIDER_SERVICE)
                    .required(true)
                    .build();
    public static final PropertyDescriptor TASK_ID =
            new PropertyDescriptor.Builder()
                    .name("awsTaskId")
                    .displayName("AWS Task ID")
                    .defaultValue("${awsTaskId}")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
                    .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Upon successful completion, the original FlowFile will be routed to this relationship.")
            .autoTerminateDefault(true)
            .build();
    public static final Relationship REL_RUNNING = new Relationship.Builder()
            .name("running")
            .description("The job is currently still being processed")
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Job successfully finished. FlowFile will be routed to this relation.")
            .build();
    public static final Relationship REL_THROTTLED = new Relationship.Builder()
            .name("throttled")
            .description("Retrieving results failed for some reason, but the issue is likely to resolve on its own, such as Provisioned Throughput Exceeded or a Throttling failure. " +
                    "It is generally expected to retry this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("The job failed, the original FlowFile will be routed to this relationship.")
            .autoTerminateDefault(true)
            .build();
    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .displayName("Region")
            .name("aws-region")
            .required(true)
            .allowableValues(getAvailableRegions())
            .defaultValue(createAllowableValue(Regions.DEFAULT_REGION).getValue())
            .build();
    public static final String FAILURE_REASON_ATTRIBUTE = "failure.reason";
    protected static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            TASK_ID,
            MANDATORY_AWS_CREDENTIALS_PROVIDER_SERVICE,
            REGION,
            TIMEOUT,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            PROXY_CONFIGURATION_SERVICE));
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .build();

    static {
        SimpleModule awsResponseModule = new SimpleModule();
        awsResponseModule.addDeserializer(ResponseMetadata.class, new AwsResponseMetadataDeserializer());
        SimpleModule sdkHttpModule = new SimpleModule();
        awsResponseModule.addDeserializer(SdkHttpMetadata.class, new SdkHttpMetadataDeserializer());
        MAPPER.registerModule(awsResponseModule);
        MAPPER.registerModule(sdkHttpModule);
    }


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_ORIGINAL,
            REL_SUCCESS,
            REL_RUNNING,
            REL_THROTTLED,
            REL_FAILURE
    )));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected T createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
        throw new UnsupportedOperationException("Client creation not supported");
    }

    protected void writeToFlowFile(ProcessSession session, FlowFile flowFile, Object response) {
        session.write(flowFile, out -> MAPPER.writeValue(out, response));
    }
}
