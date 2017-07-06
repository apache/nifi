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
import org.apache.nifi.processor.util.put.AbstractPutEventProcessor;
import org.apache.nifi.processor.util.put.sender.ChannelSender;
import org.apache.nifi.processor.util.put.sender.SocketChannelSender;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StopWatch;

import javax.net.ssl.SSLContext;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
 * <li><b>Outgoing Message Delimiter</b> - A string to append to the end of each FlowFiles content to indicate the end of the message to the TCP server.</li>
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
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final int timeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final int bufferSize = context.getProperty(MAX_SOCKET_SEND_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final SSLContextService sslContextService = (SSLContextService) context.getProperty(SSL_CONTEXT_SERVICE).asControllerService();

        SSLContext sslContext = null;
        if (sslContextService != null) {
            sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
        }

        return createSender(protocol, hostname, port, timeout, bufferSize, sslContext);
    }

    /**
     * Creates a Universal Resource Identifier (URI) for this processor. Constructs a URI of the form TCP://< host >:< port > where the host and port
     * values are taken from the configured property values.
     *
     * @param context
     *            - the current process context.
     *
     * @return The URI value as a String.
     */
    @Override
    protected String createTransitUri(final ProcessContext context) {
        final String protocol = TCP_VALUE.getValue();
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();

        return new StringBuilder().append(protocol).append("://").append(host).append(":").append(port).toString();
    }

    /**
     * Get the additional properties that are used by this processor.
     *
     * @return List of PropertyDescriptors describing the additional properties.
     */
    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(CONNECTION_PER_FLOWFILE,
                OUTGOING_MESSAGE_DELIMITER,
                TIMEOUT,
                SSL_CONTEXT_SERVICE,
                CHARSET);
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

        // really shouldn't happen since we know the protocol is TCP here, but this is more graceful so we
        // can cast to a SocketChannelSender later in order to obtain the OutputStream
        if (!(sender instanceof SocketChannelSender)) {
            getLogger().error("Processor can only be used with a SocketChannelSender, but obtained: " + sender.getClass().getCanonicalName());
            context.yield();
            return;
        }

        boolean closeSender = isConnectionPerFlowFile(context);
        try {
            // We might keep the connection open across invocations of the processor so don't auto-close this
            final OutputStream out = ((SocketChannelSender)sender).getOutputStream();
            final String delimiter = getOutgoingMessageDelimiter(context, flowFile);

            final StopWatch stopWatch = new StopWatch(true);
            try (final InputStream rawIn = session.read(flowFile);
                 final BufferedInputStream in = new BufferedInputStream(rawIn)) {
                IOUtils.copy(in, out);
                if (delimiter != null) {
                    final Charset charSet = Charset.forName(context.getProperty(CHARSET).getValue());
                    out.write(delimiter.getBytes(charSet), 0, delimiter.length());
                }
                out.flush();
            } catch (final Exception e) {
                closeSender = true;
                throw e;
            }

            session.getProvenanceReporter().send(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
            session.commit();
        } catch (Exception e) {
            onFailure(context, session, flowFile);
            getLogger().error("Exception while handling a process session, transferring {} to failure.", new Object[] { flowFile }, e);
        } finally {
            if (closeSender) {
                getLogger().debug("Closing sender");
                sender.close();
            } else {
                getLogger().debug("Relinquishing sender");
                relinquishSender(sender);
            }
        }
    }

    /**
     * Event handler method to perform the required actions when a failure has occurred. The FlowFile is penalized, forwarded to the failure relationship and the context is yielded.
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
