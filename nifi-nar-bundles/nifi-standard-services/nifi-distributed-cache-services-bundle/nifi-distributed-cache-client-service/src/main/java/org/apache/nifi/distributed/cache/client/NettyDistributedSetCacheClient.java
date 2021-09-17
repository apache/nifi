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

import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.adapter.BooleanInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.OutboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.VoidInboundAdapter;
import org.apache.nifi.distributed.cache.operations.SetOperation;
import org.apache.nifi.remote.VersionNegotiatorFactory;

import java.io.IOException;

/**
 * The implementation of the {@link DistributedCacheClient} using the netty library to provide the remote
 * communication services.
 */
public class NettyDistributedSetCacheClient extends DistributedCacheClient {

    /**
     * Constructor.
     *
     * @param context the NiFi configuration to be applied to the channel pool
     * @param factory creator of object used to broker the version of the distributed cache protocol with the service
     */
    public NettyDistributedSetCacheClient(ConfigurationContext context, VersionNegotiatorFactory factory) {
        super(context, factory);
    }

    /**
     * Adds the specified value to the cache.
     *
     * @param value the value to be added
     * @return true if the value was added to the cache, false if the value
     * already existed in the cache
     * @throws IOException if unable to communicate with the remote instance
     */
    public boolean addIfAbsent(final byte[] value) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .write(SetOperation.ADD_IF_ABSENT.value()).write(value);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    /**
     * Check for the existence of the specified value in the cache.
     *
     * @param value the value to be checked
     * @return true iff the value exists in the cache
     * @throws IOException if unable to communicate with the remote instance
     */
    public boolean contains(final byte[] value) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .write(SetOperation.CONTAINS.value()).write(value);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    /**
     * Removes the given value from the cache, if it is present.
     *
     * @param value the value to be removed
     * @return true iff the value existed in the cache
     * @throws IOException if unable to communicate with the remote instance
     */
    public boolean remove(final byte[] value) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .write(SetOperation.REMOVE.value()).write(value);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    /**
     * Perform a clean shutdown of the cache client.
     *
     * @throws IOException if unable to communicate with the remote instance
     */
    public void close() throws IOException {
        invoke(new OutboundAdapter().write(SetOperation.CLOSE.value()), new VoidInboundAdapter());
        closeChannelPool();
    }
}
