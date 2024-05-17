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
package org.apache.nifi.processors.network;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processors.network.util.PCAP;
import org.apache.nifi.processors.network.util.PCAP.ByteBufferInterface;
import org.apache.nifi.processors.network.util.PCAP.Packet;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@SideEffectFree
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"PCAP", "Splitter", "Network", "Packet", "Capture", "Wireshark", "TShark"})
@CapabilityDescription("Splits a pcap file into multiple pcap files based on a maximum size.")
@WritesAttributes({
    @WritesAttribute(attribute = SplitPCAP.ERROR_REASON, description = "The reason the flowfile was sent to the failure relationship."),
    @WritesAttribute(attribute = "fragment.identifier", description = "All split FlowFiles produced from the same parent FlowFile will have the same randomly generated UUID added for this attribute"),
    @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split FlowFiles that were created from a single parent FlowFile"),
    @WritesAttribute(attribute = "fragment.count", description = "The number of split FlowFiles generated from the parent FlowFile"),
    @WritesAttribute(attribute = "segment.original.filename ", description = "The filename of the parent FlowFile")
})

public class SplitPCAP extends AbstractProcessor {

    protected static final String ERROR_REASON = "ERROR_REASON";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final PropertyDescriptor PCAP_MAX_SIZE = new PropertyDescriptor
            .Builder().name("PCAP_MAX_SIZE")
            .displayName("PCAP max size (bytes)")
            .description("Maximum size of the output pcap file in bytes.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to "
            + "this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
            + "the unchanged FlowFile will be routed to this relationship.")
            .build();
    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("The individual 'segments' of the original FlowFile will be routed to this relationship.")
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(PCAP_MAX_SIZE);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_ORIGINAL, REL_FAILURE, REL_SPLIT);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
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
            session.putAttribute(flowFile, ERROR_REASON, "PCAP file empty.");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final PCAP parsedPcap;
        final PCAP templatePcap;

        // Parse the pcap file and create a template pcap object to borrow the header from.
        try {
            parsedPcap = new PCAP(new ByteBufferInterface(contentByteArray));

            // Recreating rather than using deepcopy as recreating is more efficient in this case.
            templatePcap = new PCAP(new ByteBufferInterface(contentByteArray));

        } catch (Exception e){
            session.putAttribute(flowFile, ERROR_REASON, "PCAP file not parseable.");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final List<Packet> unprocessedPackets = parsedPcap.packets();
        final int PCAPHeaderLength = 24;
        final int packetHeaderLength = 16;

        int currentPacketCollectionSize = PCAPHeaderLength;
        int totalFlowfileCount = 1;
        List<FlowFile> splitFilesList = new ArrayList<>();

        List<Packet> newPackets = new ArrayList<>();
        templatePcap.packets().clear();


        // Loop through all packets in the pcap file and split them into smaller pcap files.
        while (!unprocessedPackets.isEmpty()){
            Packet packet = unprocessedPackets.getFirst();

            if (packet.inclLen() > pcapMaxSize){
                session.putAttribute(flowFile, ERROR_REASON, "PCAP contains a packet larger than the max size.");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            if (currentPacketCollectionSize + (packet.inclLen() + packetHeaderLength) > pcapMaxSize && currentPacketCollectionSize > 0) {
                templatePcap.packets().addAll(newPackets);
                var newFlowFile = session.create(flowFile);

                session.write(newFlowFile, out -> out.write(templatePcap.readBytesFull()));

                session.putAttribute(
                    newFlowFile,
                    "filename",
                    flowFile.getAttribute("filename").split("\\.")[0] + "-" + totalFlowfileCount + ".pcap"
                );

                splitFilesList.add(newFlowFile);

                totalFlowfileCount += 1;

                newPackets = new ArrayList<>();
                currentPacketCollectionSize = PCAPHeaderLength;
                templatePcap.packets().clear();

            } else {
                newPackets.add(packet);
                currentPacketCollectionSize += ((int) packet.inclLen() + packetHeaderLength);
                unprocessedPackets.removeFirst();
            }
        }

        // If there are any packets left over, create a new flowfile.
        if(!newPackets.isEmpty()){
            templatePcap.packets().addAll(newPackets);
            var newFlowFile = session.create(flowFile);
            session.putAttribute(
                newFlowFile,
                "filename",
                flowFile.getAttribute("filename").split("\\.")[0] + "-" + totalFlowfileCount + ".pcap"
            );

            session.write(newFlowFile, out -> out.write(templatePcap.readBytesFull()));
            splitFilesList.add(newFlowFile);
        }

        final String fragmentId = UUID.randomUUID().toString();

        int fragmentIndex = 0;
        final String originalFileName = flowFile.getAttribute("filename");

        for(FlowFile split : splitFilesList){
            session.putAttribute(split, FRAGMENT_COUNT, String.valueOf(splitFilesList.size()));
            session.putAttribute(split, FRAGMENT_ID, fragmentId);
            session.putAttribute(split, FRAGMENT_INDEX, Integer.toString(fragmentIndex));
            session.putAttribute(split, SEGMENT_ORIGINAL_FILENAME, originalFileName);
            fragmentIndex++;
            session.transfer(split, REL_SPLIT);
        }

        session.transfer(flowFile, REL_ORIGINAL);
    }
}
