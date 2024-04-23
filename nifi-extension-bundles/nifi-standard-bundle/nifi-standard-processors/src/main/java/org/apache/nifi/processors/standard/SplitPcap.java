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
package org.apache.nifi.processors.standard;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.Pcap;
import org.apache.nifi.processors.standard.util.Pcap.ByteBufferInterface;
import org.apache.nifi.processors.standard.util.Pcap.Packet;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"PCAP", "Splitter", "Network", "Packet", "Capture", "Wireshark", "TShark"})
@CapabilityDescription("Splits a pcap file into multiple pcap files based on a maximum size.")
@WritesAttribute(attribute = "ERROR_REASON", description = "The reason the flowfile was sent to the failure relationship.")

public class SplitPcap extends AbstractProcessor {

    public static final PropertyDescriptor PCAP_MAX_SIZE = new PropertyDescriptor
            .Builder().name("PCAP_MAX_SIZE")
            .displayName("PCAP max size (bytes)")
            .description("Maximum size of the output pcap file in bytes.")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("output flowfiles")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Flowfiles not parseable as pcap.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(PCAP_MAX_SIZE);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
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

    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * This method is called when a trigger event occurs in the processor.
     * It processes the incoming flow file, splits it into smaller pcap files based on the maximum size,
     * and transfers the split pcap files to the success relationship.
     * If the flow file is empty or not parseable, it is transferred to the failure relationship.
     *
     * @param context  the process context
     * @param session  the process session
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        int pcapMaxSize = context.getProperty(PCAP_MAX_SIZE.getName()).asInteger();

        final ByteArrayOutputStream contentBytes = new ByteArrayOutputStream();
        session.exportTo(flowFile, contentBytes);
        final byte[] contentByteArray = contentBytes.toByteArray();

        if(contentByteArray.length == 0){
            session.putAttribute(flowFile,"ERROR_REASON", "PCAP file empty.");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        Pcap parsedPcap;
        Pcap templatePcap;

        // Parse the pcap file and create a template pcap object to borrow the header from.
        try{
            parsedPcap = new Pcap(new ByteBufferInterface(contentByteArray));

            // Recreating rather than using deepcopy as recreating is more efficient in this case.
            templatePcap = new Pcap(new ByteBufferInterface(contentByteArray));

        } catch (Exception e){
            session.putAttribute(flowFile,"ERROR_REASON", "PCAP file not parseable.");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        var unprocessedPackets = parsedPcap.packets();

        int currentPacketCollectionSize = 0;
        int totalFlowfileCount = 1;
        int packetHeaderLength = 24;

        ArrayList<Packet> newPackets = new ArrayList<Packet>();
        templatePcap.packets().clear();


        // Loop through all packets in the pcap file and split them into smaller pcap files.
        while (unprocessedPackets.isEmpty() != true){
            var packet = unprocessedPackets.get(0);

            if (packet.inclLen() > pcapMaxSize){
                session.putAttribute(flowFile,"ERROR_REASON", "PCAP contains a packet larger than the max size.");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            if (currentPacketCollectionSize + (packet.inclLen() + packetHeaderLength) > pcapMaxSize && currentPacketCollectionSize > 0){
                templatePcap.packets().addAll(newPackets);
                var newFlowFile = session.create(flowFile);

                session.write(newFlowFile, out -> {
                    out.write(templatePcap.readBytesFull());
                });

                session.putAttribute(
                    newFlowFile,
                    "filename",
                    flowFile.getAttribute("filename").split("\\.")[0] + "-" + totalFlowfileCount + ".pcap"
                );

                session.transfer(newFlowFile, REL_SUCCESS);
                totalFlowfileCount += 1;

                newPackets = new ArrayList<Packet>();
                currentPacketCollectionSize = 0;
                templatePcap.packets().clear();

            } else {
                newPackets.add(packet);
                currentPacketCollectionSize += (packet.inclLen() + packetHeaderLength);
                unprocessedPackets.remove(0);
            }
        }

        // If there are any packets left over, create a new flowfile.
        if(newPackets.size() > 0){
            templatePcap.packets().addAll(newPackets);
            var newFlowFile = session.create(flowFile);
            session.putAttribute(
                newFlowFile,
                "filename",
                flowFile.getAttribute("filename").split("\\.")[0] + "-" + totalFlowfileCount + ".pcap"
            );

            session.write(newFlowFile, out -> {
                out.write(templatePcap.readBytesFull());
            });
            session.transfer(newFlowFile, REL_SUCCESS);
        }

        session.remove(flowFile);
    }
}
