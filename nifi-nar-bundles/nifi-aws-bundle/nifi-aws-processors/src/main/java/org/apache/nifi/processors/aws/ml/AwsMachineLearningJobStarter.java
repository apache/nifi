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

package org.apache.nifi.processors.aws.ml;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

public abstract class AwsMachineLearningJobStarter<T extends AmazonWebServiceClient, REQUEST extends AmazonWebServiceRequest, RESPONSE extends AmazonWebServiceResult>
        extends AbstractAWSCredentialsProviderProcessor<T> {
    protected static final String AWS_TASK_ID_PROPERTY = "awsTaskId";
    public static final PropertyDescriptor JSON_PAYLOAD = new PropertyDescriptor.Builder()
            .name("json-payload")
            .displayName("JSON Payload")
            .description("JSON request for AWS Machine Learning services. The Processor will use FlowFile content for the request when this property is not specified.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MANDATORY_AWS_CREDENTIALS_PROVIDER_SERVICE =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(AWS_CREDENTIALS_PROVIDER_SERVICE)
                    .required(true)
                    .build();
    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .displayName("Region")
            .name("aws-region")
            .required(true)
            .allowableValues(getAvailableRegions())
            .defaultValue(createAllowableValue(Regions.DEFAULT_REGION).getValue())
            .build();
    protected static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            JSON_PAYLOAD,
            MANDATORY_AWS_CREDENTIALS_PROVIDER_SERVICE,
            REGION,
            TIMEOUT,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE));
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Upon successful completion, the original FlowFile will be routed to this relationship.")
            .autoTerminateDefault(true)
            .build();
    private final ObjectMapper mapper = JsonMapper.builder()
            .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
            .build();

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_ORIGINAL,
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final RESPONSE response;
        FlowFile childFlowFile;
        try {
            response = sendRequest(buildRequest(session, context, flowFile), context);
            childFlowFile = writeToFlowFile(session, flowFile, response);
            postProcessFlowFile(context, session, childFlowFile, response);
        } catch (Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Sending AWS ML Request failed", e);
            return;
        }
        session.transfer(flowFile, REL_ORIGINAL);
        session.transfer(childFlowFile, REL_SUCCESS);
    }

    protected void postProcessFlowFile(ProcessContext context, ProcessSession session, FlowFile flowFile, RESPONSE response) {
        session.putAttribute(flowFile, AWS_TASK_ID_PROPERTY, getAwsTaskId(context, response));
        getLogger().debug("AWS ML task has been started with task id: {}", getAwsTaskId(context, response));
    }

    protected REQUEST buildRequest(ProcessSession session, ProcessContext context, FlowFile flowFile) throws JsonProcessingException {
        return mapper.readValue(getPayload(session, context, flowFile), getAwsRequestClass(context));
    }

    private String getPayload(ProcessSession session, ProcessContext context, FlowFile flowFile) {
        String payloadPropertyValue = context.getProperty(JSON_PAYLOAD).evaluateAttributeExpressions(flowFile).getValue();
        if (payloadPropertyValue == null) {
            payloadPropertyValue = readFlowFile(session, flowFile);
        }
        return payloadPropertyValue;
    }

    @Override
    protected T createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
        throw new UnsupportedOperationException("createClient(ProcessContext, AWSCredentials, ClientConfiguration) is not supported");
    }

    protected FlowFile writeToFlowFile(ProcessSession session, FlowFile flowFile, RESPONSE response) {
        FlowFile childFlowFile = session.create(flowFile);
        childFlowFile = session.write(childFlowFile, out -> mapper.writeValue(out, response));
        return childFlowFile;
    }

    protected String readFlowFile(final ProcessSession session, final FlowFile flowFile) {
        try (InputStream inputStream = session.read(flowFile)) {
            return new String(IOUtils.toByteArray(inputStream));
        } catch (final IOException e) {
            throw new ProcessException("Read FlowFile Failed", e);
        }
    }

    abstract protected RESPONSE sendRequest(REQUEST request, ProcessContext context) throws JsonProcessingException;

    abstract protected Class<? extends REQUEST> getAwsRequestClass(ProcessContext context);

    abstract protected String getAwsTaskId(ProcessContext context, RESPONSE response);
}
