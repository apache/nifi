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
package org.apache.nifi.processors.irc;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.irc.handlers.ConsumerEventHandler;

@Tags({"consume", "irc"})
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("This processor implements a IRC client allowing nifi to listen for predifined IRC channel"
        + "and convert all received messages into flowfiles."
        + "Due to the nature of IRC no delivery guarantees are offered. USE WITH CARE")

@WritesAttributes({
        @WritesAttribute(attribute = "irc.sender", description = "The IRC nickname source user of the message"),
        @WritesAttribute(attribute = "irc.channel", description = "The channel from where the message was received "),
        @WritesAttribute(attribute = "irc.server", description = "The values IRC channel where the message was received from")})
public class ConsumeIRC extends AbstractIRCProcessor {
    public static PropertyDescriptor IRC_PROCESS_PRIV_MESSAGES = new PropertyDescriptor.Builder()
            .name("IRC_PROCESS_PRIV_MESSAGES")
            .displayName("Process Private Message")
            .description("Defines if the processor should discard private messages or to process them as FlowFiles.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .build();

    public static PropertyDescriptor IRC_STRIP_FORMATTING = new PropertyDescriptor.Builder()
            .name("IRC_STRIP_FORMATTING")
            .displayName("Strip message formatting")
            .description("Defines if the processor should strip message formatting when writing content to FlowFile")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .required(true)
            .build();

    private volatile ConsumerEventHandler eventHandler;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(IRC_CLIENT_SERVICE);
        propDescs.add(IRC_CHANNEL);
        propDescs.add(IRC_PROCESS_PRIV_MESSAGES);
        propDescs.add(IRC_STRIP_FORMATTING);
        return propDescs;
    }

    @OnStopped
    public void onUnscheduled(ProcessContext context) {
        clearSetup(client, eventHandler);
        this.client = null;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory ) throws ProcessException {
        if (client == null) {
            // Initialise client
            // Initialise the handler that will be provided to the setupClient super method
            this.eventHandler = new ConsumerEventHandler(context, sessionFactory, getLogger());
            // initialize the client
            this.client = setupClient(context, sessionFactory, ircClientService, eventHandler);
        }

        // Verify if should join the channel
        if (!this.client.getChannels().contains(context.getProperty(IRC_CHANNEL).getValue())) {
            ircClientService.joinChannel(context.getProperty(IRC_CHANNEL).getValue());
        }


        if (ircClientService.getIsConnected().get()) {
        }
        // Let KICL take care of the session.
        context.yield();
    }

}
