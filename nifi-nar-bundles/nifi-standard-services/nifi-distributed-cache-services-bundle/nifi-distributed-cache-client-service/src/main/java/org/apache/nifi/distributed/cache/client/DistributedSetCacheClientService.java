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

import io.netty.channel.pool.ChannelPool;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.adapter.BooleanInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.OutboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.VoidInboundAdapter;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.StandardVersionNegotiator;
import org.apache.nifi.remote.VersionNegotiator;
import org.apache.nifi.ssl.SSLContextService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Tags({"distributed", "cache", "state", "set", "cluster"})
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.server.DistributedSetCacheServer", "org.apache.nifi.ssl.StandardSSLContextService"})
@CapabilityDescription("Provides the ability to communicate with a DistributedSetCacheServer. This can be used in order to share a Set "
        + "between nodes in a NiFi cluster")
public class DistributedSetCacheClientService extends AbstractControllerService implements DistributedSetCacheClient {

    private final Logger logger = LoggerFactory.getLogger(getClass());

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
            .description("Specifies how long to wait when communicating with the remote server before determining "
                    + "that there is a communications failure if data cannot be sent or received")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .build();

    /**
     * The pool of network connections used to service client requests.
     */
    private volatile ChannelPool channelPool = null;

    /**
     * The implementation of the business logic for {@link DistributedSetCacheClientService}.
     */
    private volatile NettyDistributedCacheClient cacheClient = null;

    /**
     * Coordinator used to broker the version of the distributed cache protocol with the service.
     */
    private volatile VersionNegotiator versionNegotiator = null;

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
        logger.info("onEnabled()");
        this.versionNegotiator = new StandardVersionNegotiator(1);
        this.channelPool = NettyChannelPoolFactory.createChannelPool(context, versionNegotiator);
        this.cacheClient = new NettyDistributedCacheClient(channelPool);
    }

    @OnDisabled
    public void onDisabled() throws IOException {
        logger.info("onDisabled()");
        this.cacheClient.invoke(new OutboundAdapter().write("close"), new VoidInboundAdapter());
        this.channelPool.close();
        this.versionNegotiator = null;
        this.channelPool = null;
        this.cacheClient = null;
    }

    @OnStopped
    public void onStopped() throws IOException {
        if (isEnabled()) {
            onDisabled();
        }
    }

    @Override
    public <T> boolean addIfAbsent(T value, Serializer<T> serializer) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter().write("addIfAbsent").write(value, serializer);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        cacheClient.invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    @Override
    public <T> boolean contains(T value, Serializer<T> serializer) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter().write("contains").write(value, serializer);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        cacheClient.invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    @Override
    public <T> boolean remove(T value, Serializer<T> serializer) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter().write("remove").write(value, serializer);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        cacheClient.invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    @Override
    public void close() throws IOException {
        if (isEnabled()) {
            onDisabled();
        }
    }
}
