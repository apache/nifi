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
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.ServiceStatus;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.impl.DefaultShutdownStrategy;
import org.apache.camel.spi.ShutdownStrategy;
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
import org.apache.nifi.processor.exception.ProcessException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;

/**
 * This processor runs a Camel Route.
 */
@Tags({"camel", "route", "put"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Runs a Camel Route. Each input FlowFile is converted into a Camel Exchange "
                       + "for processing by configured Route.")
public class CamelProcessor extends AbstractProcessor {

    protected static final Relationship SUCCESS = new Relationship.Builder().name("success")
        .description("Camel Route has Executed Successfully").build();

    protected static final Relationship FAILURE = new Relationship.Builder().name("failure")
        .description("Camel Route has Failed to Execute").build();

    public static final PropertyDescriptor CAMEL_SPRING_CONTEXT_FILE_PATH = new PropertyDescriptor.Builder()
        .name("Camel Spring Config File Path")
        .description("The Classpath where NiFi can find Spring Application context file"
                         + " Ex: classpath:/META-INF/camel-application-context.xml")
        .defaultValue("classpath:/META-INF/camel-application-context.xml").required(true).addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor CAMEL_SPRING_CONTEXT_DEF = new PropertyDescriptor.Builder()
    .name("Camel Spring Context Definition")
    .description("Content of Spring Application context ")
    .defaultValue("").addValidator(Validator.VALID)
    .build();

    public static final PropertyDescriptor CAMEL_ENTRY_POINT_URI = new PropertyDescriptor.Builder()
        .name("Camel EntryPoint")
        .description("EntryPoint for NiFi in Camel Route" + " Ex: direct-vm:nifiEntryPoint")
        .defaultValue("direct-vm:nifiEntryPoint").required(true).addValidator(Validator.VALID).build();

    private static SpringCamelContext camelContext = null;

    private ImmutableList<PropertyDescriptor> descriptors;

    private ImmutableSet<Relationship> relationships=ImmutableSet.of(SUCCESS, FAILURE);

    public static synchronized SpringCamelContext getCamelContext() {
        return camelContext;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        CamelContext camelContext=getCamelContext();
        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(flowFile);
        exchange.getIn().setHeader("nifiSession", session);
        ProducerTemplate producerTemplate= camelContext.createProducerTemplate();
        producerTemplate.setDefaultEndpointUri(context.getProperty(CAMEL_ENTRY_POINT_URI)
                                                   .getValue());
        exchange = producerTemplate.send(exchange);
        try{
            producerTemplate.stop();
        }catch(Exception e){
            throw new ProcessException(e);
        }
        if (exchange != null && !(exchange.isFailed())) {
            session.transfer(exchange.getIn().getBody(FlowFile.class), SUCCESS);
        } else {
            if (exchange.isFailed() && exchange.getException() != null) {
                session.putAttribute(flowFile, "camelRouteException", exchange.getException().getMessage());
            }
            session.transfer(flowFile, FAILURE);
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = ImmutableList.of(CAMEL_SPRING_CONTEXT_FILE_PATH, CAMEL_SPRING_CONTEXT_DEF, CAMEL_ENTRY_POINT_URI);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (getCamelContext() == null
            || getCamelContext().getStatus()==ServiceStatus.Stopped
            || getCamelContext().getStatus()==ServiceStatus.Stopping) {
            try {
                String camelContextDef=context
                    .getProperty(CAMEL_SPRING_CONTEXT_DEF).getValue();
                String camelContextPath=context
                    .getProperty(CAMEL_SPRING_CONTEXT_FILE_PATH).getValue();
                ApplicationContext applicationContext;
                if(camelContextDef!=null && camelContextDef.trim().length()>0){
                    applicationContext= new GenericXmlApplicationContext(new ByteArrayResource(camelContextDef.getBytes(),"InMemory Camel COntext Definition"));
                }else{
                    if(camelContextPath.startsWith("classpath:")){
                        applicationContext=new ClassPathXmlApplicationContext(camelContextPath.split(":")[1]);
                    }else{
                        applicationContext=new FileSystemXmlApplicationContext(camelContextPath.split(":")[1]);
                    }
                }
                camelContext = new SpringCamelContext(applicationContext);
                camelContext.addStartupListener(new CamelContextStartupListener(getLogger()));
                ShutdownStrategy shutdownStrategy=new DefaultShutdownStrategy();
                shutdownStrategy.setTimeout(1);
                shutdownStrategy.setTimeUnit(TimeUnit.SECONDS);
                camelContext.setShutdownStrategy(shutdownStrategy);
                camelContext.start();
                getLogger().info("Camel Spring Context initialized");
            } catch (Exception exception) {
                getLogger().error("Failed to Shutdown Camel Spring Context", exception);
            }
        }

    }

    @OnStopped
    public void stopped(){
        if (getCamelContext() != null) {
            try {
                getCamelContext().stop();
                getCamelContext().destroy();
                ((AbstractApplicationContext)getCamelContext().getApplicationContext()).close();
            } catch (Exception e) {
               getLogger().error("Failed to Shutdown Camel Spring Context", e);
            }
        }
    }
}
