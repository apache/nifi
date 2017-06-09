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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tags({"distributed", "cache", "state", "map", "cluster"})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer", "org.apache.nifi.ssl.StandardSSLContextService"})
@CapabilityDescription("Provides the ability to communicate with a DistributedMapCacheServer. This can be used in order to share a Map "
    + "between nodes in a NiFi cluster")
public class DistributedMapCacheClientService extends AbstractControllerService implements AtomicDistributedMapCacheClient<Long> {

    private static final Logger logger = LoggerFactory.getLogger(DistributedMapCacheClientService.class);

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
        .name("Server Hostname")
        .description("The name of the server that is running the DistributedMapCacheServer service")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
        .name("Server Port")
        .description("The port on the remote server that is to be used when communicating with the DistributedMapCacheServer service")
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
        .description("Specifies how long to wait when communicating with the remote server before determining that "
                + "there is a communications failure if data cannot be sent or received")
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
    public void cacheConfig(final ConfigurationContext context) {
        this.configContext = context;
    }

    @OnStopped
    public void onStopped() throws IOException {
        close();
    }

    @Override
    public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        return withCommsSession(new CommsAction<Boolean>() {
            @Override
            public Boolean execute(final CommsSession session) throws IOException {
                final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
                dos.writeUTF("putIfAbsent");

                serialize(key, keySerializer, dos);
                serialize(value, valueSerializer, dos);

                dos.flush();

                final DataInputStream dis = new DataInputStream(session.getInputStream());
                return dis.readBoolean();
            }
        });
    }

    @Override
    public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        withCommsSession(new CommsAction<Object>() {
            @Override
            public Object execute(final CommsSession session) throws IOException {
                final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
                dos.writeUTF("put");

                serialize(key, keySerializer, dos);
                serialize(value, valueSerializer, dos);

                dos.flush();
                final DataInputStream dis = new DataInputStream(session.getInputStream());
                final boolean success = dis.readBoolean();
                if ( !success ) {
                    throw new IOException("Expected to receive confirmation of 'put' request but received unexpected response");
                }

                return null;
            }
        });
    }

    @Override
    public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
        return withCommsSession(new CommsAction<Boolean>() {
            @Override
            public Boolean execute(final CommsSession session) throws IOException {
                final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
                dos.writeUTF("containsKey");

                serialize(key, keySerializer, dos);
                dos.flush();

                final DataInputStream dis = new DataInputStream(session.getInputStream());
                return dis.readBoolean();
            }
        });
    }

    @Override
    public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer, final Deserializer<V> valueDeserializer) throws IOException {
        return withCommsSession(new CommsAction<V>() {
            @Override
            public V execute(final CommsSession session) throws IOException {
                final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
                dos.writeUTF("getAndPutIfAbsent");

                serialize(key, keySerializer, dos);
                serialize(value, valueSerializer, dos);
                dos.flush();

                // read response
                final DataInputStream dis = new DataInputStream(session.getInputStream());
                final byte[] responseBuffer = readLengthDelimitedResponse(dis);
                return valueDeserializer.deserialize(responseBuffer);
            }
        });
    }

    @Override
    public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
        return withCommsSession(new CommsAction<V>() {
            @Override
            public V execute(final CommsSession session) throws IOException {
                final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
                dos.writeUTF("get");

                serialize(key, keySerializer, dos);
                dos.flush();

                // read response
                final DataInputStream dis = new DataInputStream(session.getInputStream());
                final byte[] responseBuffer = readLengthDelimitedResponse(dis);
                return valueDeserializer.deserialize(responseBuffer);
            }
        });
    }

    @Override
    public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
        return withCommsSession(new CommsAction<Boolean>() {
            @Override
            public Boolean execute(final CommsSession session) throws IOException {
                final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
                dos.writeUTF("remove");

                serialize(key, serializer, dos);
                dos.flush();

                // read response
                final DataInputStream dis = new DataInputStream(session.getInputStream());
                return dis.readBoolean();
            }
        });
    }

    @Override
    public long removeByPattern(String regex) throws IOException {
        return withCommsSession(session -> {
            final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
            dos.writeUTF("removeByPattern");
            dos.writeUTF(regex);
            dos.flush();

            // read response
            final DataInputStream dis = new DataInputStream(session.getInputStream());
            return dis.readLong();
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> AtomicCacheEntry<K, V, Long> fetch(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        return withCommsSession(session -> {
            validateProtocolVersion(session, 2);

            final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
            dos.writeUTF("fetch");

            serialize(key, keySerializer, dos);
            dos.flush();

            // read response
            final DataInputStream dis = new DataInputStream(session.getInputStream());
            final long revision = dis.readLong();
            final byte[] responseBuffer = readLengthDelimitedResponse(dis);

            if (revision < 0) {
                // This indicates that key was not found.
                return null;
            }

            return new AtomicCacheEntry(key, valueDeserializer.deserialize(responseBuffer), revision);
        });
    }

    private void validateProtocolVersion(final CommsSession session, final int requiredProtocolVersion) {
        if (session.getProtocolVersion() < requiredProtocolVersion) {
            throw new UnsupportedOperationException("Remote cache server doesn't support protocol version " + requiredProtocolVersion);
        }
    }

    @Override
    public <K, V> boolean replace(AtomicCacheEntry<K, V, Long> entry, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        return withCommsSession(session -> {
            validateProtocolVersion(session, 2);

            final DataOutputStream dos = new DataOutputStream(session.getOutputStream());
            dos.writeUTF("replace");

            serialize(entry.getKey(), keySerializer, dos);
            dos.writeLong(entry.getRevision().orElse(0L));
            serialize(entry.getValue(), valueSerializer, dos);

            dos.flush();

            // read response
            final DataInputStream dis = new DataInputStream(session.getInputStream());
            return dis.readBoolean();
        });
    }

    private byte[] readLengthDelimitedResponse(final DataInputStream dis) throws IOException {
        final int responseLength = dis.readInt();
        final byte[] responseBuffer = new byte[responseLength];
        dis.readFully(responseBuffer);
        return responseBuffer;
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
        final VersionNegotiator versionNegotiator = new StandardVersionNegotiator(2, 1);
        try {
            ProtocolHandshake.initiateHandshake(session.getInputStream(), session.getOutputStream(), versionNegotiator);
            session.setProtocolVersion(versionNegotiator.getVersion());
        } catch (final HandshakeException e) {
            try {
                session.close();
            } catch (final IOException ioe) {
            }

            throw new IOException(e);
        }

        return session;
    }

    @Override
    public void close() throws IOException {
        this.closed = true;

        CommsSession commsSession;
        while ((commsSession = queue.poll()) != null) {
            try (final DataOutputStream dos = new DataOutputStream(commsSession.getOutputStream())) {
                dos.writeUTF("close");
                dos.flush();
                commsSession.close();
            } catch (final IOException e) {
            }
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

    private <T> void serialize(final T value, final Serializer<T> serializer, final DataOutputStream dos) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(value, baos);
        dos.writeInt(baos.size());
        baos.writeTo(dos);
    }

    private <T> T withCommsSession(final CommsAction<T> action) throws IOException {
        if (closed) {
            throw new IllegalStateException("Client is closed");
        }
        boolean tryToRequeue = true;
        final CommsSession session = leaseCommsSession();
        try {
            return action.execute(session);
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

    private static interface CommsAction<T> {

        T execute(CommsSession commsSession) throws IOException;
    }

}
