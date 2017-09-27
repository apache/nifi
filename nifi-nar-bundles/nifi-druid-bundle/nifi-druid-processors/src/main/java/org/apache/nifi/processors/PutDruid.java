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

package org.apache.nifi.processors;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.stream.io.StreamUtils;

import org.codehaus.jackson.map.ObjectMapper;

import org.apache.nifi.controller.api.DruidTranquilityService;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.twitter.util.Await;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

import scala.runtime.BoxedUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Druid", "Timeseries", "OLAP", "ingest"})
@CapabilityDescription("Sends events to Apache Druid for Indexing. "
        + "Leverages Druid Tranquility Controller service."
        + "Incoming flow files are expected to contain 1 or many JSON objects, one JSON object per line")
public class PutDruid extends AbstractSessionFactoryProcessor {

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final Map<Object, String> messageStatus = new HashMap<>();

    public static final PropertyDescriptor DRUID_TRANQUILITY_SERVICE = new PropertyDescriptor.Builder()
            .name("putdruid-tranquility-service")
            .displayName("Tranquility Service")
            .description("Tranquility Service to use for sending events to Druid")
            .required(true)
            .identifiesControllerService(DruidTranquilityService.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success relationship")
            .build();

    public static final Relationship REL_FAIL = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship when they cannot be parsed")
            .build();

    public static final Relationship REL_DROPPED = new Relationship.Builder()
            .name("dropped")
            .description("FlowFiles are routed to this relationship when they are outside of the configured time window, timestamp format is invalid, ect...")
            .build();

    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DRUID_TRANQUILITY_SERVICE);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_DROPPED);
        relationships.add(REL_FAIL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    //Method breaks down incoming flow file and sends it to Druid Indexing service
    private void processFlowFile(ProcessContext context, ProcessSession session) {
        final ComponentLog log = getLogger();

        //Get handle on Druid Tranquility session
        DruidTranquilityService tranquilityController = context.getProperty(DRUID_TRANQUILITY_SERVICE)
                .asControllerService(DruidTranquilityService.class);
        Tranquilizer<Map<String, Object>> tranquilizer = tranquilityController.getTranquilizer();

        final FlowFile flowFile = session.get();
        if (flowFile == null || flowFile.getSize() == 0) {
            return;
        }

        //Get data from flow file body
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));


        String contentString = new String(buffer, StandardCharsets.UTF_8);
        Map<String, Object> contentMap = null;

        //Create payload array from flow file content, one element per line
        String[] messageArray = contentString.split("\\R");

        //Convert each array element from JSON to HashMap and send to Druid
        for (String message : messageArray) {
            try {
                contentMap = new ObjectMapper().readValue(message, HashMap.class);
            } catch (IOException e1) {
                log.error("Error parsing incoming message array in the flowfile body");
            }

            log.debug("Tranquilizer Status: " + tranquilizer.status().toString());
            messageStatus.put(flowFile, "pending");
            //Send data element to Druid, Asynch
            Future<BoxedUnit> future = tranquilizer.send(contentMap);
            log.debug(" Sent Payload to Druid: " + contentMap);

            //Wait for Druid to call back with status
            future.addEventListener(new FutureEventListener<Object>() {
                @Override
                public void onFailure(Throwable cause) {
                    if (cause instanceof MessageDroppedException) {
                        //This happens when event timestamp targets a Druid Indexing task that has closed (Late Arriving Data)
                        log.error(" FlowFile Dropped due to MessageDroppedException: " + cause.getMessage() + " : " + cause);
                        cause.getStackTrace();
                        log.error(" Transferring FlowFile to DROPPED relationship");
                        session.transfer(flowFile, REL_DROPPED);
                    } else {
                        log.error("FlowFile Processing Failed due to: {} : {}", new Object[]{cause.getMessage(), cause});
                        cause.printStackTrace();
                        log.error(" Transferring FlowFile to FAIL relationship");
                        session.transfer(flowFile, REL_FAIL);
                    }
                }

                @Override
                public void onSuccess(Object value) {
                    log.debug(" FlowFile Processing Success : " + value.toString());
                    session.transfer(flowFile, REL_SUCCESS);
                    session.getProvenanceReporter().send(flowFile, "Druid Tranquility Service");
                }
            });

            try {
                //Wait for result from Druid
                //This method will be asynch since this is a SessionFactoryProcessor and OnTrigger will create a new Thread
                Await.result(future);
            } catch (Exception e) {
                getLogger().error("Caught exception while waiting for result of put request: " + e.getMessage());
            }
        }
        //session.transfer(flowFile, REL_SUCCESS);
        session.commit();
    }

    public void onTrigger(ProcessContext context, ProcessSessionFactory factory) throws ProcessException {
        final ProcessSession session = factory.createSession();
        //Create new Thread to ensure that waiting for callback does not reduce throughput
        new Thread(() -> {
            processFlowFile(context, session);
        }).start();
    }
}