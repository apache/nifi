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
package org.apache.nifi.processors.aws.iot;

import com.amazonaws.services.iotdata.AWSIotDataClient;
import com.amazonaws.services.iotdata.model.GetThingShadowRequest;
import com.amazonaws.services.iotdata.model.GetThingShadowResult;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;

@EventDriven
@Tags({"Amazon", "AWS", "IOT", "Shadow", "Get"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Gets last persisted state of a thing in AWS IoT by reading out the shadow. " +
        "A shadow might change more often than you get triggered. In order to get every message send " +
        "out by a thing you better use ConsumeAWSIoTMqtt processor. You can dynamically set a thing-name " +
        "when overriding the processor-configuration with a message-attribute \"aws.iot.thing.override\".")
@SeeAlso({ ConsumeAWSIoTMqtt.class })
@ReadsAttributes({
        @ReadsAttribute(attribute = "aws.iot.thing.override", description = "Overrides the processor configuration for topic."),
})
@WritesAttributes({
        @WritesAttribute(attribute = "aws.iot.thing", description = "Thing name in AWS IoT"),
})
public class GetAWSIoTShadow extends AbstractAWSIoTShadowProcessor {
    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(
                    PROP_THING,
                    AWS_CREDENTIALS_PROVIDER_SERVICE,
                    TIMEOUT,
                    PROXY_HOST,
                    PROXY_HOST_PORT,
                    REGION));

    private final static String ATTR_NAME_THING = PROP_NAME_THING + ".override";

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AWSIotDataClient iotClient = this.getClient();

        if (iotClient == null) {
            getLogger().error("AWS-Client was not initialized. See logs to find reasons.");
            return;
        }
        // get flowfile
        FlowFile flowFile = session.get();
        // if provided override configured thing name with name from corresponding message attribute
        String thingName = flowFile != null && flowFile.getAttributes().containsKey(ATTR_NAME_THING)
                        ? flowFile.getAttribute(ATTR_NAME_THING)
                        : context.getProperty(PROP_NAME_THING).getValue();

        // ask shadow of the thing for last reported state by requesting the API of AWS
        final GetThingShadowRequest iotRequest = new GetThingShadowRequest().withThingName(thingName);
        final GetThingShadowResult iotResponse = iotClient.getThingShadow(iotRequest);

        //FlowFile flowFileOut = session.create();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(PROP_NAME_THING, thingName);
        flowFile = session.putAllAttributes(flowFile, attributes);

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(final OutputStream out) throws IOException {
                out.write(iotResponse.getPayload().array());
            }
        });
        session.transfer(flowFile, REL_SUCCESS);
        session.commit();
    }
}
