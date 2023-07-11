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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeRegionsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeRegionsResponse;
import software.amazon.awssdk.services.ec2.model.Region;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
@Tags({"Amazon", "EC2", "AWS", "list"})
@CapabilityDescription("List EC2 regions visible to the user. " +
        "The processor will create a single FlowFile with the list of regions as JSON.")
public class ListEC2Regions extends AbstractEC2Processor {

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, REGION, TIMEOUT,
                    AWS_CREDENTIALS_PROVIDER_SERVICE, PROXY_HOST, PROXY_HOST_PORT,
                    PROXY_USERNAME, PROXY_PASSWORD));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        // Fetch the list of regions
        final Ec2Client client = getClient(context);
        DescribeRegionsRequest request = DescribeRegionsRequest.builder().build();
        DescribeRegionsResponse response = client.describeRegions(request);
        List<Region> regions = response.regions();

        // Write the results to an output flowfile
        if(!regions.isEmpty()){
            String json = toJson(regions);
            FlowFile flowFile = session.create();
            session.write(flowFile, (outputStream) -> {
                outputStream.write(json.getBytes());
            });
            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    //Convert list of regions to JSON using GSON
    private String toJson(List<Region> regions) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(regions);
    }
}
