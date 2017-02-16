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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.irc.IRCClientService;
import org.apache.nifi.irc.StandardIRCClientService;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.irc.handlers.EventHandler;
import org.kitteh.irc.client.library.Client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

abstract class AbstractIRCProcessor extends AbstractSessionFactoryProcessor {
    public static PropertyDescriptor IRC_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("IRC_CLIENT_SERVICE")
            .displayName("IRC Client Service")
            .description("The IRC Client Service to use with this client")
            .identifiesControllerService(StandardIRCClientService.class)
            .required(true)
            .build();
    public static PropertyDescriptor IRC_CHANNEL = new PropertyDescriptor.Builder()
            .name("IRC_CHANNEL")
            .displayName("IRC Channel")
            .description("The IRC channel this processor will consume data from")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully captured will be passed to this Relationship.")
            .build();

    protected volatile IRCClientService ircClientService;
    protected volatile Client client = null;

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(IRC_CLIENT_SERVICE);
        propDescs.add(IRC_CHANNEL);
        return propDescs;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        String IRCServiceId = context.getProperty(IRC_CLIENT_SERVICE).getValue();;
        ControllerService controllerService = context.getControllerServiceLookup().getControllerService(IRCServiceId);

        ircClientService = (IRCClientService) controllerService;
    }

    protected Client setupClient(final ProcessContext context, final ProcessSessionFactory sessionFactory, IRCClientService ircClientService, EventHandler eventHandler) {
        ircClientService.getClient().getEventManager().registerEventListener(eventHandler);
        return ircClientService.getClient();
    }

   protected void clearSetup(Client client, EventHandler eventHandler) {
        client.getEventManager().unregisterEventListener(eventHandler);
   }

}