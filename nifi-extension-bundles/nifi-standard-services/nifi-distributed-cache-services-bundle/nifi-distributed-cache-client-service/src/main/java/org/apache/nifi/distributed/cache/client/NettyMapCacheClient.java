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

import org.apache.nifi.distributed.cache.client.adapter.AtomicCacheEntryInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.BooleanInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.MapValuesInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.OutboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.SetInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.ValueInboundAdapter;
import org.apache.nifi.distributed.cache.client.adapter.VoidInboundAdapter;
import org.apache.nifi.distributed.cache.operations.MapOperation;
import org.apache.nifi.distributed.cache.protocol.ProtocolVersion;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.VersionNegotiatorFactory;
import org.apache.nifi.ssl.SSLContextProvider;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The implementation of the {@link DistributedMapCacheClient} using the netty library to provide the remote
 * communication services.
 */
public class NettyMapCacheClient extends CacheClient {
    private final ComponentLog log;

    /**
     * Constructor.
     *
     * @param hostname           the network name / IP address of the server running the distributed cache service
     * @param port               the port on which the distributed cache service is running
     * @param timeoutMillis      the network timeout associated with requests to the service
     * @param sslContextProvider the SSL context (if any) associated with requests to the service; if not specified,
     *                           communications will not be encrypted
     * @param factory            creator of object used to broker the version of the distributed cache protocol with the service
     * @param identifier         uniquely identifies this client
     * @param log                Component Log from instantiating Services
     */
    public NettyMapCacheClient(
            final String hostname,
            final int port,
            final int timeoutMillis,
            final SSLContextProvider sslContextProvider,
            final VersionNegotiatorFactory factory,
            final String identifier,
            final ComponentLog log
    ) {
        super(hostname, port, timeoutMillis, sslContextProvider, factory, identifier);
        this.log = Objects.requireNonNull(log, "Component Log required");
    }

    /**
     * Adds the specified key and value to the cache, if they are not already
     * present.
     *
     * @param key   the key to add to the map
     * @param value the value to add to the map if and only if the key is absent
     * @return true if the value was added to the cache, false if the value
     * already existed in the cache
     * @throws IOException if unable to communicate with the remote instance
     */
    public boolean putIfAbsent(final byte[] key, final byte[] value) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .write(MapOperation.PUT_IF_ABSENT.value())
                .write(key)
                .write(value);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    /**
     * Adds the specified key and value to the cache, overwriting any value that is
     * currently set.
     *
     * @param key   The key to set
     * @param value The value to associate with the given Key
     * @throws IOException if unable to communicate with the remote instance
     */
    public void put(final byte[] key, final byte[] value) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .write(MapOperation.PUT.value())
                .write(key)
                .write(value);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        invoke(outboundAdapter, inboundAdapter);
        if (!inboundAdapter.getResult()) {
            throw new IOException("Server indicated 'put' operation failed");
        }
    }

    /**
     * Determines if the given value is present in the cache and if so returns
     * <code>true</code>, else returns <code>false</code>
     *
     * @param key key
     * @return Determines if the given value is present in the cache and if so returns
     * <code>true</code>, else returns <code>false</code>
     * @throws IOException if unable to communicate with the remote instance
     */
    public boolean containsKey(final byte[] key) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .write(MapOperation.CONTAINS_KEY.value())
                .write(key);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    /**
     * Adds the specified key and value to the cache, if they are not already
     * present. If a value already exists in the cache for the given
     * key, the value associated with the key is returned.
     *
     * @param key          the key to add to the map
     * @param value        the value to add to the map if and only if the key is absent
     * @param valueAdapter the reader used to deserialize the service result
     * @return If a value already exists in the cache for the given
     * key, the value associated with the key is returned. If the key does not
     * exist, the key and its value will be added to the cache.
     * @throws IOException if unable to communicate with the remote instance
     */
    public <V> V getAndPutIfAbsent(final byte[] key, final byte[] value,
                                   final ValueInboundAdapter<V> valueAdapter) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .write(MapOperation.GET_AND_PUT_IF_ABSENT.value())
                .write(key)
                .write(value);
        invoke(outboundAdapter, valueAdapter);
        return valueAdapter.getResult();
    }

    /**
     * Returns the value in the cache for the given key, if one exists;
     * otherwise returns <code>null</code>
     *
     * @param <V>          the value type
     * @param key          the key to lookup in the map
     * @param valueAdapter the reader used to deserialize the service result
     * @return the value in the cache for the given key
     * @throws IOException if unable to communicate with the remote instance
     */
    public <V> V get(final byte[] key, final ValueInboundAdapter<V> valueAdapter) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .write(MapOperation.GET.value())
                .write(key);
        invoke(outboundAdapter, valueAdapter);
        return valueAdapter.getResult();
    }

    /**
     * Returns the values in the cache for the given keys, if they exist.
     *
     * @param <K>        the key type
     * @param <V>        the value type
     * @param keys       a set of keys whose values to lookup in the map
     * @param mapAdapter the reader used to deserialize the service result
     * @return the value in the cache for the given key, if one exists
     * @throws IOException if unable to communicate with the remote instance
     */
    public <K, V> Map<K, V> subMap(Collection<byte[]> keys, final MapValuesInboundAdapter<K, V> mapAdapter) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .minimumVersion(ProtocolVersion.V3.value())
                .write(MapOperation.SUBMAP.value())
                .write(keys);
        invoke(outboundAdapter, mapAdapter);
        return mapAdapter.getResult();
    }

    /**
     * Removes the entry with the given key from the cache, if it is present.
     *
     * @param key the key to remove from the map
     * @return <code>true</code> if the entry is removed, <code>false</code> if
     * the key did not exist in the cache
     * @throws IOException if unable to communicate with the remote instance
     */
    public boolean remove(final byte[] key) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .write(MapOperation.REMOVE.value())
                .write(key);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    /**
     * Removes the entry with the given key from the cache, if it is present,
     * and returns the value that was removed from the map.
     *
     * @param <V>          type of value
     * @param key          the key to remove from the map
     * @param valueAdapter the reader used to deserialize the service result
     * @return the value previously associated with the key, or null if there was no mapping
     * @throws IOException if unable to communicate with the remote instance
     */
    public <V> V removeAndGet(final byte[] key, final ValueInboundAdapter<V> valueAdapter) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .minimumVersion(ProtocolVersion.V3.value())
                .write(MapOperation.REMOVE_AND_GET.value())
                .write(key);
        invoke(outboundAdapter, valueAdapter);
        return valueAdapter.getResult();
    }

    /**
     * Fetch a CacheEntry with a key.
     *
     * @param <K>            the key type
     * @param <V>            the value type
     * @param key            the key to lookup in the map
     * @param inboundAdapter the reader used to deserialize the service result
     * @return A CacheEntry instance if one exists
     * @throws IOException if unable to communicate with the remote instance
     */
    public <K, V> AtomicCacheEntry<K, V, Long> fetch(
            final byte[] key, final AtomicCacheEntryInboundAdapter<K, V> inboundAdapter) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .minimumVersion(ProtocolVersion.V2.value())
                .write(MapOperation.FETCH.value())
                .write(key);
        invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    /**
     * Replace an existing key with new value.
     *
     * @param key      the key of the map entry to be replaced
     * @param value    the new value
     * @param revision the expected revision of the map entry; on no match, value will not be replaced
     * @return true only if the key is replaced
     * @throws IOException if unable to communicate with the remote instance
     */
    public boolean replace(final byte[] key, final byte[] value, final long revision) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .minimumVersion(ProtocolVersion.V2.value())
                .write(MapOperation.REPLACE.value())
                .write(key)
                .write(revision)
                .write(value);
        final BooleanInboundAdapter inboundAdapter = new BooleanInboundAdapter();
        invoke(outboundAdapter, inboundAdapter);
        return inboundAdapter.getResult();
    }

    /**
     * Returns a set of all keys currently in the cache.
     *
     * @param <K>        type of key
     * @param setAdapter the reader used to deserialize the service result
     * @return a Set of all keys currently in the cache
     * @throws IOException if unable to communicate with the remote instance
     */
    public <K> Set<K> keySet(final SetInboundAdapter<K> setAdapter) throws IOException {
        final OutboundAdapter outboundAdapter = new OutboundAdapter()
                .minimumVersion(ProtocolVersion.V3.value())
                .write(MapOperation.KEYSET.value());
        invoke(outboundAdapter, setAdapter);
        return setAdapter.getResult();
    }

    /**
     * Perform a clean shutdown of the cache client.
     *
     * @throws IOException if unable to communicate with the remote instance
     */
    public void close() throws IOException {
        try {
            invoke(new OutboundAdapter().write(MapOperation.CLOSE.value()), new VoidInboundAdapter());
        } catch (final Exception e) {
            log.warn("Sending close command failed: closing channel", e);
        }
        closeChannelPool();
    }
}
