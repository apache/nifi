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
package org.apache.nifi.distributed.cache.server.codec;

import io.netty.buffer.ByteBuf;
import org.apache.nifi.distributed.cache.operations.CacheOperation;
import org.apache.nifi.distributed.cache.operations.MapOperation;
import org.apache.nifi.distributed.cache.server.protocol.MapCacheRequest;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Cache Request Decoder processes bytes and decodes cache version and operation requests
 */
public class MapCacheRequestDecoder extends CacheRequestDecoder {

    public MapCacheRequestDecoder(
            final ComponentLog log,
            final int maxLength,
            final CacheOperation[] supportedOperations
    ) {
        super(log, maxLength, supportedOperations);
    }

    @Override
    protected Optional<Object> readRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final MapCacheRequest request;

        if (MapOperation.CONTAINS_KEY == cacheOperation) {
            request = readKeyRequest(cacheOperation, byteBuf);
        } else if (MapOperation.FETCH == cacheOperation) {
            request = readKeyRequest(cacheOperation, byteBuf);
        } else if (MapOperation.GET == cacheOperation) {
            request = readKeyRequest(cacheOperation, byteBuf);
        } else if (MapOperation.GET_AND_PUT_IF_ABSENT == cacheOperation) {
            request = readKeyValueRequest(cacheOperation, byteBuf);
        } else if (MapOperation.KEYSET == cacheOperation) {
            request = new MapCacheRequest(cacheOperation);
        } else if (MapOperation.REMOVE == cacheOperation) {
            request = readKeyRequest(cacheOperation, byteBuf);
        } else if (MapOperation.REMOVE_AND_GET == cacheOperation) {
            request = readKeyRequest(cacheOperation, byteBuf);
        } else if (MapOperation.REPLACE == cacheOperation) {
            request = readKeyRevisionValueRequest(cacheOperation, byteBuf);
        } else if (MapOperation.SUBMAP == cacheOperation) {
            request = readSubMapRequest(cacheOperation, byteBuf);
        } else if (MapOperation.PUT == cacheOperation) {
            request = readKeyValueRequest(cacheOperation, byteBuf);
        } else if (MapOperation.PUT_IF_ABSENT == cacheOperation) {
            request = readKeyValueRequest(cacheOperation, byteBuf);
        } else {
            request = new MapCacheRequest(cacheOperation);
        }

        return Optional.ofNullable(request);
    }

    private MapCacheRequest readKeyRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final Optional<byte[]> key = readBytes(byteBuf);
        return key.map(bytes -> new MapCacheRequest(cacheOperation, bytes)).orElse(null);
    }

    private MapCacheRequest readKeyValueRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final MapCacheRequest mapCacheRequest;

        final Optional<byte[]> key = readBytes(byteBuf);
        if (key.isPresent()) {
            final Optional<byte[]> value = readBytes(byteBuf);
            mapCacheRequest = value.map(valueBytes -> new MapCacheRequest(cacheOperation, key.get(), valueBytes)).orElse(null);
        } else {
            mapCacheRequest = null;
        }

        return mapCacheRequest;
    }

    private MapCacheRequest readKeyRevisionValueRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final MapCacheRequest mapCacheRequest;

        final Optional<byte[]> key = readBytes(byteBuf);
        if (key.isPresent()) {
            final OptionalLong revision = readLong(byteBuf);
            if (revision.isPresent()) {
                final Optional<byte[]> value = readBytes(byteBuf);
                mapCacheRequest = value.map(valueBytes -> new MapCacheRequest(cacheOperation, key.get(), revision.getAsLong(), valueBytes)).orElse(null);
            } else {
                mapCacheRequest = null;
            }
        } else {
            mapCacheRequest = null;
        }

        return mapCacheRequest;
    }

    private MapCacheRequest readSubMapRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final MapCacheRequest mapCacheRequest;

        final OptionalInt keys = readInt(byteBuf);
        if (keys.isPresent()) {
            final List<byte[]> subMapKeys = new ArrayList<>();
            for (int i = 0; i < keys.getAsInt(); i++) {
                final Optional<byte[]> key = readBytes(byteBuf);
                if (key.isPresent()) {
                    subMapKeys.add(key.get());
                } else {
                    // Clear Map to return null and retry on subsequent invocations
                    subMapKeys.clear();
                    break;
                }

            }

            mapCacheRequest = subMapKeys.isEmpty() ? null : new MapCacheRequest(cacheOperation, subMapKeys);
        } else {
            mapCacheRequest = null;
        }

        return mapCacheRequest;
    }
}
