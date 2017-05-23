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
package org.apache.nifi.distributed.cache.client;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.protocol.ProtocolHandshake;
import org.apache.nifi.distributed.cache.protocol.exception.HandshakeException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.DataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tags({"distributed", "cache", "state", "set", "cluster"})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.server.DistributedSetCacheServer", "org.apache.nifi.ssl.StandardSSLContextService"})
@CapabilityDescription("Provides the ability to communicate with a DistributedSetCacheServer. This can be used in order to share a Set "
        + "between nodes in a NiFi cluster")
public class DistributedSetCacheClientService extends AbstractControllerService implements DistributedSetCacheClient {

    private static final Logger logger = LoggerFactory.getLogger(DistributedMapCacheClientService.class);

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Server Hostname")
            .description("The name of the server that is running the DistributedSetCacheServer service")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Server Port")
            .description("The port on the remote server that is to be used when communicating with the DistributedSetCacheServer service")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .defaultValue("4557")
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("If specified, indicates the SSL Context Service that is used to communicate with the "
                    + "remote server. If not specified, communications will not be encrypted")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    public static final PropertyDescriptor COMMUNICATIONS_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .description("Specifices how long to wait when communicating with the remote server before determining "
                    + "that there is a communications failure if data cannot be sent or received")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .build();

    private final BlockingQueue<CommsSession> queue = new LinkedBlockingQueue<>();
    private volatile ConfigurationContext configContext;
    private volatile boolean closed = false;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOSTNAME);
        descriptors.add(PORT);
        descriptors.add(SSL_CONTEXT_SERVICE);
        descriptors.add(COMMUNICATIONS_TIMEOUT);
        return descriptors;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        this.configContext = context;
    }

    @OnStopped
    public void onStopped() throws IOException {
        close();
    }

    public CommsSession createCommsSession(final ConfigurationContext context) throws IOException {
        final String hostname = context.getProperty(HOSTNAME).getValue();
        final int port = context.getProperty(PORT).asInteger();
        final int timeoutMillis = context.getProperty(COMMUNICATIONS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        final CommsSession commsSession;
        if (sslContextService == null) {
            commsSession = new StandardCommsSession(hostname, port, timeoutMillis);
        } else {
            commsSession = new SSLCommsSession(sslContextService.createSSLContext(ClientAuth.REQUIRED), hostname, port, timeoutMillis);
        }

        commsSession.setTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
        return commsSession;
    }

    private CommsSession leaseCommsSession() throws IOException {
        CommsSession session = queue.poll();
        if (session != null && !session.isClosed()) {
            return session;
        }

        session = createCommsSession(configContext);
        final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(1);
        try {
            ProtocolHandshake.initiateHandshake(session.getInputStream(), session.getOutputStream(), versionNegotiator);
            session.setProtocolVersion(versionNegotiator.getVersion());
        } catch (final HandshakeException e) {
            IOUtils.closeQuietly(session);

            throw new IOException(e);
        }

        return session;
    }

    @Override
    public <T> boolean addIfAbsent(final T value, final Serializer<T> serializer) throws IOException {
        return invokeRemoteBoolean("addIfAbsent", value, serializer);
    }

    @Override
    public <T> boolean contains(final T value, final Serializer<T> serializer) throws IOException {
        return invokeRemoteBoolean("contains", value, serializer);
    }

    @Override
    public <T> boolean remove(final T value, final Serializer<T> serializer) throws IOException {
        return invokeRemoteBoolean("remove", value, serializer);
    }

    @Override
    public void close() throws IOException {
        this.closed = true;

        CommsSession commsSession;
        while ((commsSession = queue.poll()) != null) {
            try (final DataOutputStream dos = new DataOutputStream(commsSession.getOutputStream())) {
                dos.writeUTF("close");
                dos.flush();
            } catch (final IOException e) {
            }
            IOUtils.closeQuietly(commsSession);
        }
        if (logger.isDebugEnabled() && getIdentifier() != null) {
            logger.debug("Closed {}", new Object[]{getIdentifier()});
        }
    }

    @Override
    protected void finalize() throws Throwable {
        if (!closed) {
            close();
        }
        logger.debug("Finalize called");
    }

    private <T> boolean invokeRemoteBoolean(final String methodName, final T value, final Serializer<T> serializer) throws IOException {
        if (closed) {
            throw new IllegalStateException("Client is closed");
        }

        final CommsSession session = leaseCommsSession();
        boolean tryToRequeue = true;
        try {
            final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
            dos.writeUTF(methodName);

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            serializer.serialize(value, baos);
            dos.writeInt(baos.size());
            baos.writeTo(dos);
            dos.flush();

            final DataInputStream dis = new DataInputStream(session.getInputStream());
            return dis.readBoolean();
        } catch (final IOException ioe) {
            tryToRequeue = false;
            throw ioe;
        } finally {
            if (tryToRequeue == true && this.closed == false) {
                queue.offer(session);
            } else {
                IOUtils.closeQuietly(session);
            }
        }
    }
}
