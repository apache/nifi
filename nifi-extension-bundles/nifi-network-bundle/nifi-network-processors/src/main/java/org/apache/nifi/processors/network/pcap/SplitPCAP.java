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
package org.apache.nifi.processors.network.pcap;

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
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@SideEffectFree
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"PCAP", "Splitter", "Network", "Packet", "Capture", "Wireshark", "TShark", "TcpDump", "WinDump", "sniffers"})
@CapabilityDescription("Splits a pcap file into multiple pcap files based on a maximum size.")
@WritesAttributes({
    @WritesAttribute(
        attribute = SplitPCAP.ERROR_REASON,
        description = "The reason the flowfile was sent to the failure relationship."
    ),
    @WritesAttribute(
        attribute = "fragment.identifier",
        description = "All split PCAP FlowFiles produced from the same parent PCAP FlowFile will have the same randomly generated UUID added for this attribute"
    ),
    @WritesAttribute(
        attribute = "fragment.index",
        description = "A one-up number that indicates the ordering of the split PCAP FlowFiles that were created from a single parent PCAP FlowFile"
    ),
    @WritesAttribute(
        attribute = "fragment.count",
        description = "The number of split PCAP FlowFiles generated from the parent PCAP FlowFile"
    ),
    @WritesAttribute(
        attribute = "segment.original.filename ",
        description = "The filename of the parent PCAP FlowFile"
    )
})

public class SplitPCAP extends AbstractProcessor {

    protected static final String ERROR_REASON = "ERROR_REASON";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final PropertyDescriptor PCAP_MAX_SIZE = new PropertyDescriptor
            .Builder().name("PCAP Max Size")
            .displayName("PCAP Max Size")
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
            .description("The individual PCAP 'segments' of the original PCAP FlowFile will be routed to this relationship.")
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
     * It processes the incoming flow file, splits it into smaller pcap files based on the PCAP Max Size,
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

        final int pcapMaxSize = context.getProperty(PCAP_MAX_SIZE.getName()).asInteger();

        final ByteArrayOutputStream contentBytes = new ByteArrayOutputStream();
        session.exportTo(flowFile, contentBytes);
        final byte[] contentByteArray = contentBytes.toByteArray();

        if (contentByteArray.length == 0) {
            session.putAttribute(flowFile, ERROR_REASON, "PCAP file empty.");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final PCAP parsedPcap;
        final PCAP templatePcap;

        // Parse the pcap file and create a template pcap object to borrow the header from.
        try {
            parsedPcap = new PCAP(new ByteBufferInterface(contentByteArray));

            // Recreating rather than using deep copy as recreating is more efficient in this case.
            templatePcap = new PCAP(new ByteBufferInterface(contentByteArray));

        } catch (Exception e) {
            session.putAttribute(flowFile, ERROR_REASON, "PCAP file not parseable.");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final List<Packet> unprocessedPackets = parsedPcap.packets();
        final List<FlowFile> splitFilesList = new ArrayList<>();
        int currentPacketCollectionSize = PCAP.PCAP_HEADER_LENGTH;
        List<Packet> newPackets = new ArrayList<>();
        templatePcap.packets().clear();

        // Loop through all packets in the pcap file and split them into smaller pcap files.
        while (!unprocessedPackets.isEmpty()) {
            Packet packet = unprocessedPackets.getFirst();

            int currentPacketTotalLength = PCAP.PACKET_HEADER_LENGTH + packet.rawBody().length;

            if ((packet.inclLen() + PCAP.PACKET_HEADER_LENGTH) > pcapMaxSize) {
                session.putAttribute(flowFile, ERROR_REASON, "PCAP contains a packet larger than the max size.");
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            if ((currentPacketCollectionSize + currentPacketTotalLength) > pcapMaxSize && currentPacketCollectionSize > 0) {
                templatePcap.packets().addAll(newPackets);
                var newFlowFile = session.create(flowFile);
                session.write(newFlowFile, out -> out.write(templatePcap.readBytesFull()));
                splitFilesList.add(newFlowFile);

                newPackets = new ArrayList<>();
                currentPacketCollectionSize = PCAP.PCAP_HEADER_LENGTH;
                templatePcap.packets().clear();
            } else {
                newPackets.add(packet);
                currentPacketCollectionSize += currentPacketTotalLength;
                unprocessedPackets.removeFirst();
            }
        }

        // If there are any packets left over, create a new flowfile.
        if (!newPackets.isEmpty()) {
            templatePcap.packets().addAll(newPackets);
            var newFlowFile = session.create(flowFile);
            session.write(newFlowFile, out -> out.write(templatePcap.readBytesFull()));
            splitFilesList.add(newFlowFile);
        }

        final String fragmentId = UUID.randomUUID().toString();
        final String originalFileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        final String originalFileNameWithoutExtension = originalFileName.substring(0, originalFileName.lastIndexOf("."));

        IntStream.range(0, splitFilesList.size()).forEach(index -> {
            FlowFile split = splitFilesList.get(index);
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), originalFileNameWithoutExtension + "-" + index + ".pcap");
            attributes.put(FRAGMENT_COUNT, String.valueOf(splitFilesList.size()));
            attributes.put(FRAGMENT_ID, fragmentId);
            attributes.put(FRAGMENT_INDEX, Integer.toString(index));
            attributes.put(SEGMENT_ORIGINAL_FILENAME, originalFileName);
            session.putAllAttributes(split, attributes);
        });
        session.transfer(splitFilesList, REL_SPLIT);
        session.transfer(flowFile, REL_ORIGINAL);
    }
}
