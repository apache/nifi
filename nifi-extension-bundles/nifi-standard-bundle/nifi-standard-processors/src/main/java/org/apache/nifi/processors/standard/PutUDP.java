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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.ByteArrayNettyEventSenderFactory;
import org.apache.nifi.event.transport.netty.NettyEventSenderFactory;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.put.AbstractPutEventProcessor;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

@CapabilityDescription("The PutUDP processor receives a FlowFile and packages the FlowFile content into a single UDP datagram packet which is then transmitted to the configured UDP server."
        + " The user must ensure that the FlowFile content being fed to this processor is not larger than the maximum size for the underlying UDP transport. The maximum transport size will "
        + "vary based on the platform setup but is generally just under 64KB. FlowFiles will be marked as failed if their content is larger than the maximum transport size.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SeeAlso({ListenUDP.class, PutTCP.class})
@Tags({ "remote", "egress", "put", "udp" })
@SupportsBatching
public class PutUDP extends AbstractPutEventProcessor<byte[]> {

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        try {
            final byte[] content = readContent(session, flowFile);
            eventSender.sendEvent(content);

            session.getProvenanceReporter().send(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
            session.commitAsync();
        } catch (final Exception e) {
            getLogger().error("Send Failed {}", flowFile, e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            session.commitAsync();
            context.yield();
        }
    }

    @Override
    protected String getProtocol(final ProcessContext context) {
        return UDP_VALUE.getValue();
    }

    @Override
    protected NettyEventSenderFactory<byte[]> getNettyEventSenderFactory(final String hostname, final int port, final String protocol) {
        return new ByteArrayNettyEventSenderFactory(getLogger(), hostname, port, TransportProtocol.UDP);
    }

    private byte[] readContent(final ProcessSession session, final FlowFile flowFile) throws IOException {
        try (final InputStream inputStream = session.read(flowFile)) {
            return IOUtils.toByteArray(inputStream);
        }
    }
}
