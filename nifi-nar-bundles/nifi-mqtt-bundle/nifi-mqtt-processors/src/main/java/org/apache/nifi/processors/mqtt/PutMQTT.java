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

package org.apache.nifi.processors.mqtt;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.commons.io.IOUtils;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.io.InputStream;
import java.io.IOException;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"PutMQTT"})
@CapabilityDescription("Publishes message to an MQTT topic")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="topic", description="Topic to publish message to")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class PutMQTT extends AbstractProcessor implements MqttCallback {

    String broker;
    String clientID;
    
    MemoryPersistence persistence = new MemoryPersistence();
    MqttClient mqttClient;
    
    public static final PropertyDescriptor PROPERTY_BROKER_ADDRESS = new PropertyDescriptor
            .Builder().name("Broker address")
            .description("MQTT broker address (e.g. tcp://localhost:1883)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor PROPERTY_MQTT_CLIENTID = new PropertyDescriptor
            .Builder().name("MQTT client ID")
            .description("MQTT client ID to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    
    @Override
    public void connectionLost(Throwable t) {
	getLogger().info("Connection to " + broker + " lost");
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROPERTY_BROKER_ADDRESS);
        descriptors.add(PROPERTY_MQTT_CLIENTID);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            broker = context.getProperty(PROPERTY_BROKER_ADDRESS).getValue();
            clientID = context.getProperty(PROPERTY_MQTT_CLIENTID).getValue();
            mqttClient = new MqttClient(broker, clientID, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            mqttClient.setCallback(this);
            connOpts.setCleanSession(true);
            getLogger().info("Connecting to broker: " + broker);
            mqttClient.connect(connOpts);
        } catch(MqttException me) {
            getLogger().error("msg "+me.getMessage());
        }
    }
 
    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        try {
            mqttClient.disconnect();
        } catch(MqttException me) {
            
        }
        getLogger().error("Disconnected");
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AtomicReference<String> message = new AtomicReference<>();

        FlowFile flowfile = session.get();
        message.set("");

        // get the MQTT topic
        
        String topic = flowfile.getAttribute("topic");

        if (topic == null) {
            getLogger().error("No topic attribute on flowfile");
            session.remove(flowfile);
            return;
        }
        
        // do the read
        
        session.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                    message.set(IOUtils.toString(in));
                }catch(Exception e){
                    getLogger().error("Failed to read flowfile " + e.getMessage());
                }
            }
        });
        try {
            session.remove(flowfile);
        } catch (Exception e) {
             getLogger().error("Failed to remove flowfile " + e.getMessage());
             return;
        }       
       
        String output = message.get();
        
        if ((output == null) || output.isEmpty()) {
            return;
        }
        
        try {
            mqttClient.publish(topic, output.getBytes(), 0, false);
        } catch(MqttException me) {
            getLogger().error("msg "+me.getMessage());          
        }       
    }
}
