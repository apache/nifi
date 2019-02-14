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
package org.apache.nifi.processors.oraclecdc;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.binary.Base32;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.nifi.oraclecdcservice.api.OracleCDCEventHandler;
import org.nifi.oraclecdcservice.api.OracleCDCService;

@TriggerSerially
@Tags({ "Oracle", "CDC", "change capture" })
@CapabilityDescription("capture oracle CDC changes using xstream api. "
        + "Look at this link to see how to setup xstream outbound server on oracle db"
        + " https://github.com/rkarthik29/oracle_cdc/blob/master/README.md")
@SeeAlso({})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "cdc_type", description = "provides the type of cdc operation, "
        + "ex.INSERT,UPDATE,DELETE,COMMIT") })
@Stateful(scopes = Scope.CLUSTER, description = "After receiving cdc events from the xstream out server "
        + "the processor will store the last position received in the state map. "
        + "It will be used to set the low watermark for the next call")

public class OracleChangeCapture extends AbstractProcessor {

    public static final PropertyDescriptor XS_OUT = new PropertyDescriptor.Builder().name("XS_OUT")
            .displayName("XStream Outbound Server Name").description("xout").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor CDC_SERVICE = new PropertyDescriptor.Builder()
            .name("Oracle CDC connection pooling service")
            .description("The Controller Service that is used to obtain connection to database").required(true)
            .identifiesControllerService(OracleCDCService.class).build();

    public static final Relationship INSERTS = new Relationship.Builder().name("INSERTS").description("CDC INSERTS")
            .build();

    public static final Relationship UPDATES = new Relationship.Builder().name("UPDATES").description("CDC UPDATES")
            .build();

    public static final Relationship DELETES = new Relationship.Builder().name("DELETES").description("CDC DELETES")
            .build();
    public static final Relationship UNMATCHED = new Relationship.Builder().name("UNMATCHED")
            .description("UNSUPPORTED OPERATION").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    protected DBCPService dbcpService;

    protected Object xsOut;

    private OracleCDCService cdcService;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(XS_OUT);
        descriptors.add(CDC_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(INSERTS);
        relationships.add(UPDATES);
        relationships.add(DELETES);
        relationships.add(UNMATCHED);
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
        byte[] position = null;
        final StateManager stateManager = context.getStateManager();
        StateMap stateMap;
        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            stateMap = null;
            getLogger().warn("Failed to retrieve the low watermark from State Manager", ioe);
        }
        if (stateMap != null) {
            System.out.println("position from statemap" + stateMap.get("positon"));
            position = new Base32(true).decode(stateMap.get("positon"));
        }
        cdcService = context.getProperty(CDC_SERVICE).asControllerService(OracleCDCService.class);
        String xsOutServerName = context.getProperty(XS_OUT).getValue();
        xsOut = cdcService.attach(xsOutServerName, position);

    }

    @OnShutdown
    @OnStopped
    public void shutdown(final ProcessContext context) {
        cdcService.detach(xsOut);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        CDCHandler handler = new CDCHandler(session);
        cdcService.receiveEvents(xsOut, handler);
        if (handler.isMoveMarker()) {
            cdcService.setProcessedLowWaterMark(xsOut, handler.getLastPosition());

            final StateManager stateManager = context.getStateManager();
            try {
                HashMap<String, String> stateMap = new HashMap<String, String>();
                System.out.println(
                        "setting position in SM" + new String(new Base32(true).encode(handler.getLastPosition())));
                stateMap.put("position", new String(new Base32(true).encode(handler.getLastPosition())));
                stateManager.setState(stateMap, Scope.CLUSTER);
            } catch (final IOException ioe) {
                getLogger().warn("Failed to set the low watermark to State Manager", ioe);
            }
        }
    }

    class CDCHandler implements OracleCDCEventHandler {
        private byte[] lastPosition = null;
        private boolean moveMarker = false;

        private final ProcessSession session;

        public CDCHandler(final ProcessSession session) {
            this.session = session;
        }

        @Override
        public void inserts(String events, byte[] position) {
            // TODO Auto-generated method stub
            FlowFile flowFile = createFF(events);
            flowFile = session.putAttribute(flowFile, "cdc_type", "INSERT");
            session.transfer(flowFile, INSERTS);
            this.lastPosition = position;
            this.moveMarker = true;
        }

        @Override
        public void updates(String events, byte[] position) {
            FlowFile flowFile = createFF(events);
            flowFile = session.putAttribute(flowFile, "cdc_type", "UPDATE");
            session.transfer(flowFile, UPDATES);
            this.lastPosition = position;
            this.moveMarker = true;

        }

        @Override
        public void deletes(String events, byte[] position) {
            FlowFile flowFile = createFF(events);
            flowFile = session.putAttribute(flowFile, "cdc_type", "DELETE");
            session.transfer(flowFile, DELETES);
            this.lastPosition = position;
            this.moveMarker = true;

        }

        @Override
        public void other(String events, byte[] position) {
            FlowFile flowFile = createFF(events);
            flowFile = session.putAttribute(flowFile, "cdc_type", "UNMATCHED");
            session.transfer(flowFile, UNMATCHED);
            this.lastPosition = position;
            this.moveMarker = true;

        }

        private FlowFile createFF(String events) {
            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, new OutputStreamCallback() {

                @Override
                public void process(OutputStream outputStream) throws IOException {
                    // TODO Auto-generated method stub
                    outputStream.write(events.getBytes());
                }
            });
            return flowFile;
        }

        public byte[] getLastPosition() {
            return lastPosition;
        }

        public boolean isMoveMarker() {
            return moveMarker;
        }

    }

}
