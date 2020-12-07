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
package org.apache.nifi.processors.splunk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.splunk.RequestMessage;
import com.splunk.ResponseMessage;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dto.splunk.EventIndexStatusRequest;
import org.apache.nifi.dto.splunk.EventIndexStatusResponse;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"splunk", "logs", "http", "acknowledgement"})
@CapabilityDescription("Queries Splunk server in order to acquire the status of indexing acknowledgement.")
public class QuerySplunkIndexingStatus extends SplunkAPICall {
    private static final String ENDPOINT = "/services/collector/ack";

    static final Relationship RELATIONSHIP_ACKNOWLEDGED = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred into this relationship when the acknowledgement was successful.")
            .build();

    static final Relationship RELATIONSHIP_UNACKNOWLEDGED = new Relationship.Builder()
            .name("unacknowledged")
            .description("A FlowFile is transferred into this relationship when the acknowledgement was not successful.")
            .build();

    static final Relationship RELATIONSHIP_UNDETERMINED = new Relationship.Builder()
            .name("undetermined")
            .description(
                    "A FlowFile is transferred into this relationship when the acknowledgement state is not determined. " +
                    "Flow files transferred into this relationship might be penalized!")
            .build();

    static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is transferred into this relationship when the acknowledgement was not successful.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            RELATIONSHIP_ACKNOWLEDGED,
            RELATIONSHIP_UNACKNOWLEDGED,
            RELATIONSHIP_UNDETERMINED,
            RELATIONSHIP_FAILURE
    )));

    static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("ttl")
            .displayName("Maximum waiting time")
            .description(
                    "The maximum time the service tries to acquire acknowledgement confirmation for an index, from the point of registration. " +
                    "After the given amount of time, the service considers the index as not acknowledged and moves it into the output buffer as failed acknowledgement.")
            .defaultValue("100 secs")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor MAX_QUERY_SIZE = new PropertyDescriptor.Builder()
            .name("max-query-size")
            .displayName("Maximum Query Size")
            .description(
                    "The maximum number of acknowledgement identifiers the service query status for in one batch. " +
                    "It is suggested to not set it too low in order to reduce network communication.")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private volatile Integer maxQuerySize;
    private volatile Integer ttl;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> result = new ArrayList<>();
        final List<PropertyDescriptor> common = super.getSupportedPropertyDescriptors();
        result.addAll(common);
        result.add(TTL);
        result.add(MAX_QUERY_SIZE);
        return result;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        maxQuerySize = context.getProperty(MAX_QUERY_SIZE).asInteger();
        ttl = context.getProperty(TTL).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        super.onUnscheduled();
        maxQuerySize = null;
        ttl = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final long currentTime = System.currentTimeMillis();
        final List<FlowFile> flowFiles = session.get(maxQuerySize);
        final Map<Integer, FlowFile> undetermined = new HashMap<>();

        for (final FlowFile flowFile : flowFiles)  {
            final String insertedAt = flowFile.getAttribute(insertedAtAttributeName);
            final String ackId = flowFile.getAttribute(ackIdAttributeName);

            if (ackId == null || insertedAt == null) {
                getLogger().error("Flow file attributes \"" + insertedAtAttributeName + "\" and \"" + ackIdAttributeName + "\" are needed!");
                session.transfer(flowFile, RELATIONSHIP_FAILURE);
            } else if (Long.valueOf(insertedAt) + ttl < currentTime) {
                session.transfer(flowFile, RELATIONSHIP_UNACKNOWLEDGED);
            } else {
                undetermined.put(Integer.valueOf(flowFile.getAttribute(ackIdAttributeName)), flowFile);
            }
        }

        if (undetermined.isEmpty()) {
            getLogger().info("There was no eligible flow file to send request to Splunk.");
            return;
        }

        try {
            final RequestMessage requestMessage = createRequestMessage(undetermined);

            final ResponseMessage responseMessage = call(ENDPOINT, requestMessage);

            if (responseMessage.getStatus() == 200) {
                final EventIndexStatusResponse splunkResponse = extractResult(responseMessage.getContent(), EventIndexStatusResponse.class);

                splunkResponse.getAcks().entrySet().stream().forEach(result -> {
                    if (result.getValue()) {
                        session.transfer(undetermined.get(result.getKey()), RELATIONSHIP_ACKNOWLEDGED);
                    } else {
                        session.penalize(undetermined.get(result.getKey()));
                        session.transfer(undetermined.get(result.getKey()), RELATIONSHIP_UNDETERMINED);
                    }
                });
            } else {
                getLogger().error("Query index status was not successful because of (" + responseMessage.getStatus() + ") " + responseMessage.getContent());
                context.yield();
                session.transfer(undetermined.values(), RELATIONSHIP_UNDETERMINED);
            }
        } catch (final IOException e) {
            throw new ProcessException(e);
        }
    }

    private RequestMessage createRequestMessage(Map<Integer, FlowFile> undetermined) throws JsonProcessingException {
        final RequestMessage requestMessage = new RequestMessage("POST");
        requestMessage.getHeader().put("Content-Type", "application/json");
        requestMessage.setContent(generateContent(undetermined));
        return requestMessage;
    }

    private String generateContent(final Map<Integer, FlowFile> undetermined) throws JsonProcessingException {
        final EventIndexStatusRequest splunkRequest = new EventIndexStatusRequest();
        splunkRequest.setAcks(undetermined.keySet().stream().collect(Collectors.toList()));
        return jsonObjectMapper.writeValueAsString(splunkRequest);
    }
}
