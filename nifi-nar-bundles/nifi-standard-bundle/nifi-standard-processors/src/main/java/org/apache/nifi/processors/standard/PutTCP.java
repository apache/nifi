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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.put.AbstractPutEventProcessor;
import org.apache.nifi.processor.util.put.sender.ChannelSender;
import org.apache.nifi.stream.io.StreamUtils;

/**
 * <p>
 * The PutTCP processor receives a FlowFile and transmits the FlowFile content over a TCP connection to the configured TCP server. By default, the FlowFiles are transmitted over the same TCP
 * connection (or pool of TCP connections if multiple input threads are configured). To assist the TCP server with determining message boundaries, an optional "Outgoing Message Delimiter" string can
 * be configured which is appended to the end of each FlowFiles content when it is transmitted over the TCP connection. An optional "Connection Per FlowFile" parameter can be specified to change the
 * behaviour so that each FlowFiles content is transmitted over a single TCP connection which is opened when the FlowFile is received and closed after the FlowFile has been sent. This option should
 * only be used for low message volume scenarios, otherwise the platform may run out of TCP sockets.
 * </p>
 *
 * <p>
 * This processor has the following required properties:
 * <ul>
 * <li><b>Hostname</b> - The IP address or host name of the destination TCP server.</li>
 * <li><b>Port</b> - The TCP port of the destination TCP server.</li>
 * </ul>
 * </p>
 * 
 * <p>
 * This processor has the following optional properties:
 * <ul>
 * <li><b>Connection Per FlowFile</b> - Specifies that each FlowFiles content will be transmitted on a separate TCP connection.</li>
 * <li><b>Idle Connection Expiration</b> - The time threshold after which a TCP sender is deemed eligible for pruning - the associated TCP connection will be closed after this timeout.</li>
 * <li><b>Max Size of Socket Send Buffer</b> - The maximum size of the socket send buffer that should be used. This is a suggestion to the Operating System to indicate how big the socket buffer should
 * be. If this value is set too low, the buffer may fill up before the data can be read, and incoming data will be dropped.</li>
 * <li><b>Outgoing Message Delimiter/b> - A string to append to the end of each FlowFiles content to indicate the end of the message to the TCP server.</li>
 * <li><b>Timeout</b> - The timeout period for determining an error has occurred whilst connecting or sending data.</li>
 * </ul>
 * </p>
 *
 * <p>
 * The following relationships are required:
 * <ul>
 * <li><b>failure</b> - Where to route FlowFiles that failed to be sent.</li>
 * <li><b>success</b> - Where to route FlowFiles after they were successfully sent to the TCP server.</li>
 * </ul>
 * </p>
 *
 */
@CapabilityDescription("The PutTCP processor receives a FlowFile and transmits the FlowFile content over a TCP connection to the configured TCP server. "
        + "By default, the FlowFiles are transmitted over the same TCP connection (or pool of TCP connections if multiple input threads are configured). "
        + "To assist the TCP server with determining message boundaries, an optional \"Outgoing Message Delimiter\" string can be configured which is appended "
        + "to the end of each FlowFiles content when it is transmitted over the TCP connection. An optional \"Connection Per FlowFile\" parameter can be "
        + "specified to change the behaviour so that each FlowFiles content is transmitted over a single TCP connection which is opened when the FlowFile "
        + "is received and closed after the FlowFile has been sent. This option should only be used for low message volume scenarios, otherwise the platform " + "may run out of TCP sockets.")
@InputRequirement(Requirement.INPUT_REQUIRED)
@SeeAlso(ListenTCP.class)
@Tags({ "remote", "egress", "put", "tcp" })
@TriggerWhenEmpty // trigger even when queue is empty so that the processor can check for idle senders to prune.
public class PutTCP extends AbstractPutEventProcessor {

    /**
     * Creates a concrete instance of a ChannelSender object to use for sending messages over a TCP stream.
     *
     * @param context
     *            - the current process context.
     *
     * @return ChannelSender object.
     */
    @Override
    protected ChannelSender createSender(final ProcessContext context) throws IOException {
        final String protocol = TCP_VALUE.getValue();
        final String hostname = context.getProperty(HOSTNAME).getValue();
        final int port = context.getProperty(PORT).asInteger();
        final int timeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final int bufferSize = context.getProperty(MAX_SOCKET_SEND_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();

        return createSender(protocol, hostname, port, timeout, bufferSize, null);
    }

    /**
     * Creates a Universal Resource Identifier (URI) for this processor. Constructs a URI of the form TCP://<host>:<port> where the host and port values are taken from the configured property values.
     *
     * @param context
     *            - the current process context.
     *
     * @return The URI value as a String.
     */
    @Override
    protected String createTransitUri(final ProcessContext context) {
        final String protocol = TCP_VALUE.getValue();
        final String host = context.getProperty(HOSTNAME).getValue();
        final String port = context.getProperty(PORT).getValue();

        return new StringBuilder().append(protocol).append("://").append(host).append(":").append(port).toString();
    }

    /**
     * Get the additional properties that are used by this processor.
     *
     * @return List of PropertyDescriptors describing the additional properties.
     */
    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(CONNECTION_PER_FLOWFILE, OUTGOING_MESSAGE_DELIMITER, TIMEOUT);
    }

    /**
     * event handler method to handle the FlowFile being forwarded to the Processor by the framework. The FlowFile contents is sent out over a TCP connection using an acquired ChannelSender object. If
     * the FlowFile contents was sent out successfully then the FlowFile is forwarded to the success relationship. If an error occurred then the FlowFile is forwarded to the failure relationship.
     *
     * @param context
     *            - the current process context.
     *
     * @param sessionFactory
     *            - a factory object to obtain a process session.
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            pruneIdleSenders(context.getProperty(IDLE_EXPIRATION).asTimePeriod(TimeUnit.MILLISECONDS).longValue());
            context.yield();
            return;
        }

        ChannelSender sender = acquireSender(context, session, flowFile);
        if (sender == null) {
            return;
        }

        try {
            String delimiter = getDelimiter(context, flowFile);
            ByteArrayOutputStream content = readContent(session, flowFile);
            if (delimiter != null) {
                content = appendDelimiter(content, delimiter);
            }
            sender.send(content.toByteArray());
            session.transfer(flowFile, REL_SUCCESS);
            session.commit();
        } catch (Exception e) {
            getLogger().error("Exception while handling a process session, transferring {} to failure.", new Object[] { flowFile }, e);
            onFailure(context, session, flowFile);
        } finally {
            // If we are going to use this sender again, then relinquish it back to the pool.
            if (!isConnectionPerFlowFile(context)) {
                relinquishSender(sender);
            } else {
                sender.close();
            }
        }
    }

    /**
     * event handler method to perform the required actions when a failure has occurred. The FlowFile is penalized, forwarded to the failure relationship and the context is yielded.
     *
     * @param context
     *            - the current process context.
     *
     * @param session
     *            - the current process session.
     * @param flowFile
     *            - the FlowFile that has failed to have been processed.
     */
    protected void onFailure(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        session.transfer(session.penalize(flowFile), REL_FAILURE);
        session.commit();
        context.yield();
    }

    /**
     * Helper method to read the FlowFile content stream into a ByteArrayOutputStream object.
     *
     * @param session
     *            - the current process session.
     * @param flowFile
     *            - the FlowFile to read the content from.
     *
     * @return ByteArrayOutputStream object containing the FlowFile content.
     */
    protected ByteArrayOutputStream readContent(final ProcessSession session, final FlowFile flowFile) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream((int) flowFile.getSize() + 1);
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.copy(in, baos);
            }
        });

        return baos;
    }

    /**
     * Helper method to append a delimiter to the message contents.
     * 
     * @param content
     *            - the message contents.
     * @param delimiter
     *            - the delimiter value.
     *
     * @return ByteArrayOutputStream object containing the new message contents.
     */
    protected ByteArrayOutputStream appendDelimiter(final ByteArrayOutputStream content, final String delimiter) {
        content.write(delimiter.getBytes(), 0, delimiter.length());
        return content;
    }

    /**
     * Gets the current value of the "Outgoing Message Delimiter" property.
     *
     * @param context
     *            - the current process context.
     * @param flowFile
     *            - the FlowFile being processed.
     *
     * @return String containing the Delimiter value.
     */
    protected String getDelimiter(final ProcessContext context, final FlowFile flowFile) {
        String delimiter = context.getProperty(OUTGOING_MESSAGE_DELIMITER).evaluateAttributeExpressions(flowFile).getValue();
        if (delimiter != null) {
            delimiter = delimiter.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        }
        return delimiter;
    }

    /**
     * Gets the current value of the "Connection Per FlowFile" property.
     *
     * @param context
     *            - the current process context.
     *
     * @return boolean value - true if a connection per FlowFile is specified.
     */
    protected boolean isConnectionPerFlowFile(final ProcessContext context) {
        return context.getProperty(CONNECTION_PER_FLOWFILE).getValue().equalsIgnoreCase("true");
    }
}
