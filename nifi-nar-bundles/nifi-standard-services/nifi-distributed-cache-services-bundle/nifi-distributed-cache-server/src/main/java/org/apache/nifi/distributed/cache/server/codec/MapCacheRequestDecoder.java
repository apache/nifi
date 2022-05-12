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
    protected Object readRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final Object request;

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
        } else if (MapOperation.REMOVE_BY_PATTERN == cacheOperation) {
            request = readPatternRequest(cacheOperation, byteBuf);
        } else if (MapOperation.REMOVE_BY_PATTERN_AND_GET == cacheOperation) {
            request = readPatternRequest(cacheOperation, byteBuf);
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

        return request;
    }

    private MapCacheRequest readKeyRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final byte[] key = readBytes(byteBuf);
        return key == null ? null : new MapCacheRequest(cacheOperation, key);
    }

    private MapCacheRequest readKeyValueRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final MapCacheRequest mapCacheRequest;

        final byte[] key = readBytes(byteBuf);
        if (key == null) {
            mapCacheRequest = null;
        } else {
            final byte[] value = readBytes(byteBuf);
            mapCacheRequest = value == null ? null : new MapCacheRequest(cacheOperation, key, value);
        }

        return mapCacheRequest;
    }

    private MapCacheRequest readKeyRevisionValueRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final MapCacheRequest mapCacheRequest;

        final byte[] key = readBytes(byteBuf);
        if (key == null) {
            mapCacheRequest = null;
        } else {
            final long revision = byteBuf.readLong();
            final byte[] value = readBytes(byteBuf);
            mapCacheRequest = value == null ? null : new MapCacheRequest(cacheOperation, key, revision, value);
        }

        return mapCacheRequest;
    }

    private MapCacheRequest readPatternRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final String pattern = readUnicodeString(byteBuf);
        return new MapCacheRequest(cacheOperation, pattern);
    }

    private MapCacheRequest readSubMapRequest(final CacheOperation cacheOperation, final ByteBuf byteBuf) {
        final int keys = readInt(byteBuf);

        final List<byte[]> subMapKeys = new ArrayList<>();
        for (int i = 0; i < keys; i++) {
            final byte[] key = readBytes(byteBuf);
            if (key == null) {
                break;
            }
            subMapKeys.add(key);
        }

        return new MapCacheRequest(cacheOperation, subMapKeys);
    }
}
