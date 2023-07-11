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
package org.apache.nifi.processors.aws.ec2;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import software.amazon.awssdk.services.ec2.Ec2Client;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Arrays;
import java.util.Collections;

@Tags({"Amazon", "EC2", "AWS", "list"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@WritesAttributes({
        @WritesAttribute(attribute = "aws.ec2.instances.count", description = "The number of EC2 instances returned in the flow file")
})
@CapabilityDescription("List EC2 instances in a given region. The processor will create a FlowFile for each page of results." +
        "Use Batch Size to control the number of instances per FlowFile.")
public class ListEC2Instances extends AbstractEC2Processor {
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile original = session.get();
        final Ec2Client client = getClient(context, original);
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .maxResults(context.getProperty(BATCH_SIZE).asInteger())
                        .nextToken(nextToken).build();
                DescribeInstancesResponse response = client.describeInstances(request);
                List<Instance> instances = new ArrayList<Instance>();
                for (Reservation reservation : response.reservations()) {
                    instances.addAll(reservation.instances());
                }
                if(instances.isEmpty()){
                    break;
                }
                String json = toJson(instances);
                FlowFile newFlowFile = session.write(session.create(), (outputStream) -> {
                    outputStream.write(json.getBytes());
                });
                // copy attributes from incoming flow file to outgoing flow file
                if(original != null) {
                    session.putAllAttributes(newFlowFile,original.getAttributes());
                }
                session.putAttribute(newFlowFile, "aws.ec2.instances.count", String.valueOf(instances.size()));
                session.transfer(newFlowFile, REL_SUCCESS);
                getLogger().info("Encoded {} EC2 instances in JSON to the flow file", instances.size());
                session.getProvenanceReporter().create(newFlowFile, "Replaced original flow file with EC2 instances JSON ");
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception e) {
            getLogger().error("Failed to list EC2 instances due to {}", new Object[]{e.getMessage()}, e);
            context.yield();
        }
        if(original != null) {
            session.transfer(original, REL_ORIGINAL);
        }
    }

    // convert list of EC2 Instances to JSON
    private String toJson(List<Instance> instances) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(instances);
    }

    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .name("Region")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of functions to return in a single FlowFile. This value must be between 1 and 100." +
                    "Default value is 10.")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1L, 100L, true))
            .defaultValue("10")
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE,TIMEOUT,
                    AWS_CREDENTIALS_PROVIDER_SERVICE, REGION, BATCH_SIZE, PROXY_HOST, PROXY_HOST_PORT,
                    PROXY_USERNAME, PROXY_PASSWORD));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that may have contained attributes that were parsed is always forwarded to this relationship")
            .build();

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new LinkedHashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE, REL_ORIGINAL))
    );

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

}