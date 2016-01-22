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
package org.apache.nifi.processors.camel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.spring.SpringCamelContext;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.SchedulingContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * This processor runs a Camel Route.
 */
@Tags({"camel", "route", "put"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Runs a Camel Route. Each input FlowFile is converted into a Camel Exchange "
                       + "for processing by configured Route.")
public class CamelProcessor extends AbstractProcessor {

    private static final Relationship SUCCESS = new Relationship.Builder().name("success")
        .description("Camel Route has Executed Successfully").build();

    private static final Relationship FAILURE = new Relationship.Builder().name("failure")
        .description("Camel Route has Failed to Execute").build();

    public static final PropertyDescriptor CAMEL_SPRING_CONTEXT_FILE_PATH = new PropertyDescriptor.Builder()
        .name("Camel Spring Config File Path")
        .description("The Classpath where NiFi can find Spring Application context file"
                         + " Ex: /META-INF/camel-application-context.xml")
        .defaultValue("/META-INF/camel-application-context.xml").required(true).addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor CAMEL_ENTRY_POINT_URI = new PropertyDescriptor.Builder()
        .name("Camel EntryPoint")
        .description("EntryPoint for NiFi in Camel Route" + " Ex: vm:nifiEntryPoint")
        .defaultValue("vm:nifiEntryPoint").required(true).addValidator(Validator.VALID).build();

    private static CamelContext camelContext = null;

    private ImmutableList<PropertyDescriptor> descriptors;

    private ImmutableSet<Relationship> relationships;

    public static synchronized CamelContext getCamelContext() {
        return camelContext;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile incomingFlowFile = session.get();
        if (incomingFlowFile == null) {
            return;
        }
        if (camelContext != null
            && !(camelContext.isSetupRoutes() || camelContext.isStartingRoutes() || camelContext
                .isSuspended())) {
            getLogger().info("Got hold of a running CamelContext " + camelContext.getName());
        } else {
            throw new ProcessException("Camel Route is in unusable state");
        }

        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(incomingFlowFile);
        exchange = camelContext.createProducerTemplate().send(context.getProperty(CAMEL_ENTRY_POINT_URI)
                                                                  .getValue(), exchange);
        if (exchange != null && !(exchange.isFailed())) {
            session.transfer(exchange.getIn().getBody(FlowFile.class), SUCCESS);
        } else {
            if (exchange.isFailed()) {
                incomingFlowFile.getAttributes().put("camelRouteException",
                                                exchange.getException() != null ? exchange.getException()
                                                    .toString() : null);
            }
            session.transfer(incomingFlowFile, FAILURE);
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = ImmutableList.of(CAMEL_SPRING_CONTEXT_FILE_PATH, CAMEL_ENTRY_POINT_URI);
        this.relationships = ImmutableSet.of(SUCCESS, FAILURE);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final SchedulingContext context) {
        if (getCamelContext() == null) {
            try {
                camelContext = new SpringCamelContext(new ClassPathXmlApplicationContext(context
                    .getProperty(CAMEL_SPRING_CONTEXT_FILE_PATH).getValue()));
                // camelContext.start();
                camelContext.addStartupListener(new CamelContextStartupListener(getLogger()));
                camelContext.start();
                getLogger().info("Camel Spring Context initialized");
            } catch (Exception exception) {
                getLogger().warn(exception.getLocalizedMessage(), exception);
            }
        }

    }

    @OnStopped
    public void stopped(){
        if (getCamelContext() != null) {
            try {
                getCamelContext().stop();
            } catch (Exception e) {
                // Nothing To Do
            }
        }
    }

}
