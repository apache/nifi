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
package org.apache.nifi.processors.pcap;

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.pcap4j.core.BpfProgram;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.Packet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"pcap", "packet", "network"})
@CapabilityDescription("Uses the pcap4j library to capture packets for a given network interface. Each packet is emitted as " +
        "a single FlowFile, with the content of the FlowFile being the payload of the packet. It is expected that libpcap is " +
        "already installed on the host operating system prior to using this processor, and on some operating systems it may " +
        "require running NiFi as root in order for libpcap to access the network interfaces. In some cases it may also be " +
        "necessary to specify the native libpcap filename through the pcap4j property: org.pcap4j.core.pcapLibName." +
        "Consult the pcap4j documentation for further information.")
@WritesAttributes({@WritesAttribute(attribute="packet.header", description="The header of the given packet")})
public class GetPcap extends AbstractProcessor {

    public static final PropertyDescriptor INTERFACE_NAME = new PropertyDescriptor
            .Builder().name("Interface Name")
            .description("The name of the interface to capture from (ex: en0)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BPF_EXPRESSION = new PropertyDescriptor
            .Builder().name("BPF Expression")
            .description("A Berkeley Packet Filter expression used to filter packets (ex: icmp)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SNAPSHOT_LENGTH = new PropertyDescriptor
            .Builder().name("Snapshot Length")
            .description("The amount of data in bytes to capture for each packet.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("65536 B")
            .build();

    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor
            .Builder().name("Read Timeout")
            .description("The read timeout in milliseconds. Must be non-negative. May be ignored by some OSs. " +
                    "0 means disable buffering on Solaris. 0 means infinite on the other OSs. 1 through 9 means infinite on Solaris.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 ms")
            .build();

    public static final AllowableValue PROMISCUOUS_MODE_VALUE = new AllowableValue("PROMISCUOUS", "Promiscuous");
    public static final AllowableValue NON_PROMISCUOUS_MODE_VALUE = new AllowableValue("NONPROMISCUOUS", "Non-Promiscuous");

    public static final PropertyDescriptor CAPTURE_MODE = new PropertyDescriptor
            .Builder().name("Capture Mode")
            .description("The capture mode.")
            .required(true)
            .allowableValues(PROMISCUOUS_MODE_VALUE, NON_PROMISCUOUS_MODE_VALUE)
            .defaultValue(PROMISCUOUS_MODE_VALUE.getValue())
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successful captures are route out this relationship")
            .build();

    public static final String PACKET_HEADER_ATTR = "packet.header";

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private AtomicReference<PcapNetworkInterface> pcapNetworkInterface = new AtomicReference<>(null);
    private AtomicReference<PcapHandle> pcapHandle = new AtomicReference<>(null);
    private final BlockingQueue<Packet> packetQueue = new LinkedBlockingQueue<>(100);

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(INTERFACE_NAME);
        descriptors.add(BPF_EXPRESSION);
        descriptors.add(SNAPSHOT_LENGTH);
        descriptors.add(READ_TIMEOUT);
        descriptors.add(CAPTURE_MODE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
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
    public void onScheduled(final ProcessContext context) throws PcapNativeException, NotOpenException {
        final String interfaceName = context.getProperty(INTERFACE_NAME).getValue();
        final String filter = context.getProperty(BPF_EXPRESSION).getValue();
        final String mode = context.getProperty(CAPTURE_MODE).getValue();
        final int snapLen = context.getProperty(SNAPSHOT_LENGTH).asDataSize(DataUnit.B).intValue();
        final Long timePeriod = context.getProperty(READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        final int readTimeout = (timePeriod == null ? 0 : timePeriod.intValue());

        final PcapNetworkInterface nif = Pcaps.getDevByName(interfaceName);
        if (nif == null) {
            throw new IllegalStateException("Unable to locate Network Interface " + interfaceName);
        }

        final PcapHandle handle = nif.openLive(snapLen, PcapNetworkInterface.PromiscuousMode.valueOf(mode), readTimeout);
        if (filter != null && filter.length() > 0) {
            handle.setFilter(filter, BpfProgram.BpfCompileMode.OPTIMIZE);
        }

        final Thread listenerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    handle.loop(-1, new PacketListener() {
                        @Override
                        public void gotPacket(Packet packet) {
                            packetQueue.offer(packet);
                        }
                    });
                    getLogger().info("PcapListener shutting down...");
                } catch (PcapNativeException e) {
                    getLogger().error("Error calling pcap native libraries", e);
                } catch (NotOpenException e) {
                    getLogger().error("Pcap handle was not open", e);
                } catch (InterruptedException e) {
                    getLogger().info("PcapListener interrupted");
                } catch (Exception e) {
                    getLogger().error("Unexpected error in PcapListener", e);
                }
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.setName("PcapListener-" + getIdentifier());
        listenerThread.start();

        pcapNetworkInterface.set(nif);
        pcapHandle.set(handle);
    }

    @OnUnscheduled
    public void onUnscheduled() {
        final PcapHandle handle = pcapHandle.get();
        if (handle != null) {
            try {
                getLogger().debug("Breaking pcap loop...");
                handle.breakLoop();
            } catch (NotOpenException e) {
                getLogger().warn("Pcap handle was not open: {}", new Object[]{e.getMessage()});
            } finally {
                getLogger().debug("Closing pcap Handle...");
                handle.close();
            }
        }
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        packetQueue.clear(); // clear any queued packets as they may no longer be valid after properties have been changed
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final Packet packet = packetQueue.poll();
        if (packet == null || (packet.getPayload() == null && packet.getHeader() == null)) {
            return;
        }

        try {
            FlowFile flowFile = session.create();

            final Packet.Header header = packet.getHeader();
            if (header != null) {
                flowFile = session.putAttribute(flowFile, PACKET_HEADER_ATTR, packet.getHeader().toString());
            }

            final Packet payload = packet.getPayload();
            if (payload != null) {
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(payload.getRawData());
                        out.flush();
                    }
                });
            }

            getLogger().info("Transferring {} to success", new Object[]{flowFile});
            session.transfer(flowFile, SUCCESS);
        } catch (ProcessException pe) {
            getLogger().error("Error processing packet", pe);
            packetQueue.offer(packet); // requeue the packet so we try again
        }
    }

}
