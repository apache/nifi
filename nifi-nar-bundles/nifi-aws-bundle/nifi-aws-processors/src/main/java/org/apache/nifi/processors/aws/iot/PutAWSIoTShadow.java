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
import com.amazonaws.services.iotdata.model.UpdateThingShadowRequest;
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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;

@Tags({"Amazon", "AWS", "IOT", "Shadow", "Put", "Update", "Write"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sets state of a thing in AWS IoT by updating the shadow. You can dynamically set a thing-name " +
        "when overriding the processor-configuration with a message-attribute \"aws.iot.thing.override\".")
@SeeAlso({ PutAWSIoT.class })
@ReadsAttributes({
        @ReadsAttribute(attribute = "aws.iot.thing.override", description = "Overrides the processor configuration for topic."),
})
@WritesAttributes({
        @WritesAttribute(attribute = "aws.iot.thing", description = "Underlying MQTT quality-of-service.")
})
public class PutAWSIoTShadow extends AbstractAWSIoTShadowProcessor {
    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(
                    PROP_THING,
                    TIMEOUT,
                    AWS_CREDENTIALS_PROVIDER_SERVICE,
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
        // get flowfile
        FlowFile flowFile = session.get();
        if (flowFile == null) return;

        // if provided override configured thing name with the name from the corresponding message attribute
        String thingName =
                flowFile.getAttributes().containsKey(ATTR_NAME_THING) ?
                        flowFile.getAttribute(ATTR_NAME_THING) :
                        context.getProperty(PROP_NAME_THING).getValue();

        // get message content and put it into the flowfile
        final ByteArrayOutputStream fileContentStream = new ByteArrayOutputStream();
        session.exportTo(flowFile, fileContentStream);

        ByteBuffer buffer = ByteBuffer.wrap(fileContentStream.toByteArray());

        final AWSIotDataClient iotClient = getClient();
        final UpdateThingShadowRequest iotRequest = new UpdateThingShadowRequest()
                .withThingName(thingName)
                .withPayload(buffer);

        try {
            iotClient.updateThingShadow(iotRequest);
        } catch (final Exception e) {
            getLogger().error("Error while updating thing shadow {}; routing to failure", new Object[]{e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
        session.commit();
    }
}
