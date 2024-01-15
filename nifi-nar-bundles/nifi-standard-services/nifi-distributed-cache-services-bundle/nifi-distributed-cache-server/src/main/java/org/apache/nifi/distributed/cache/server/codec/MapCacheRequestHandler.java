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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.nifi.distributed.cache.operations.CacheOperation;
import org.apache.nifi.distributed.cache.operations.MapOperation;
import org.apache.nifi.distributed.cache.server.map.MapCache;
import org.apache.nifi.distributed.cache.server.map.MapCacheRecord;
import org.apache.nifi.distributed.cache.server.map.MapPutResult;
import org.apache.nifi.distributed.cache.server.protocol.CacheOperationResult;
import org.apache.nifi.distributed.cache.server.protocol.MapCacheRequest;
import org.apache.nifi.distributed.cache.server.protocol.MapSizeResponse;
import org.apache.nifi.distributed.cache.server.protocol.MapValueResponse;
import org.apache.nifi.logging.ComponentLog;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Handler for Map Cache Request operations interacts with the Map Cache and writes Results
 */
@ChannelHandler.Sharable
public class MapCacheRequestHandler extends SimpleChannelInboundHandler<MapCacheRequest> {
    private static final long REVISION_NOT_FOUND = -1;

    private final ComponentLog log;

    private final MapCache mapCache;

    public MapCacheRequestHandler(
            final ComponentLog log,
            final MapCache mapCache
    ) {
        this.log = Objects.requireNonNull(log, "Component Log required");
        this.mapCache = Objects.requireNonNull(mapCache, "Map Cache required");
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final MapCacheRequest mapCacheRequest) throws Exception {
        final CacheOperation cacheOperation = mapCacheRequest.getCacheOperation();

        if (MapOperation.CLOSE == cacheOperation) {
            log.debug("Map Cache Operation [{}] received", cacheOperation);
            channelHandlerContext.close();
        } else if (MapOperation.CONTAINS_KEY == cacheOperation) {
            final ByteBuffer key = ByteBuffer.wrap(mapCacheRequest.getKey());
            final boolean success = mapCache.containsKey(key);
            writeResult(channelHandlerContext, cacheOperation, success);
        } else if (MapOperation.GET == cacheOperation) {
            final ByteBuffer key = ByteBuffer.wrap(mapCacheRequest.getKey());
            final ByteBuffer cached = mapCache.get(key);
            writeBytes(channelHandlerContext, cacheOperation, cached);
        } else if (MapOperation.GET_AND_PUT_IF_ABSENT == cacheOperation) {
            final ByteBuffer key = ByteBuffer.wrap(mapCacheRequest.getKey());
            final ByteBuffer value = ByteBuffer.wrap(mapCacheRequest.getValue());
            final MapPutResult result = mapCache.putIfAbsent(key, value);
            final ByteBuffer cached = result.isSuccessful() ? null : result.getExisting().getValue();
            writeBytes(channelHandlerContext, cacheOperation, cached);
        } else if (MapOperation.FETCH == cacheOperation) {
            final ByteBuffer key = ByteBuffer.wrap(mapCacheRequest.getKey());
            final MapCacheRecord mapCacheRecord = mapCache.fetch(key);
            writeMapCacheRecord(channelHandlerContext, cacheOperation, mapCacheRecord);
        } else if (MapOperation.KEYSET == cacheOperation) {
            final Set<ByteBuffer> keySet = mapCache.keySet();
            writeSize(channelHandlerContext, cacheOperation, keySet.size());
            for (final ByteBuffer key : keySet) {
                writeBytes(channelHandlerContext, cacheOperation, key);
            }
        } else if (MapOperation.PUT == cacheOperation) {
            final ByteBuffer key = ByteBuffer.wrap(mapCacheRequest.getKey());
            final ByteBuffer value = ByteBuffer.wrap(mapCacheRequest.getValue());
            final MapPutResult result = mapCache.put(key, value);
            writeResult(channelHandlerContext, cacheOperation, result.isSuccessful());
        } else if (MapOperation.PUT_IF_ABSENT == cacheOperation) {
            final ByteBuffer key = ByteBuffer.wrap(mapCacheRequest.getKey());
            final ByteBuffer value = ByteBuffer.wrap(mapCacheRequest.getValue());
            final MapPutResult result = mapCache.putIfAbsent(key, value);
            writeResult(channelHandlerContext, cacheOperation, result.isSuccessful());
        } else if (MapOperation.REMOVE == cacheOperation) {
            final ByteBuffer key = ByteBuffer.wrap(mapCacheRequest.getKey());
            final ByteBuffer removed = mapCache.remove(key);
            final boolean success = removed != null;
            writeResult(channelHandlerContext, cacheOperation, success);
        } else if (MapOperation.REMOVE_AND_GET == cacheOperation) {
            final ByteBuffer key = ByteBuffer.wrap(mapCacheRequest.getKey());
            final ByteBuffer removed = mapCache.remove(key);
            writeBytes(channelHandlerContext, cacheOperation, removed);
        } else if (MapOperation.REPLACE == cacheOperation) {
            final ByteBuffer key = ByteBuffer.wrap(mapCacheRequest.getKey());
            final ByteBuffer value = ByteBuffer.wrap(mapCacheRequest.getValue());
            final MapCacheRecord mapCacheRecord = new MapCacheRecord(key, value, mapCacheRequest.getRevision());
            final MapPutResult result = mapCache.replace(mapCacheRecord);
            writeResult(channelHandlerContext, cacheOperation, result.isSuccessful());
        } else if (MapOperation.SUBMAP == cacheOperation) {
            final List<byte[]> keys = mapCacheRequest.getKeys();
            for (final byte[] key : keys) {
                final ByteBuffer requestedKey = ByteBuffer.wrap(key);
                final ByteBuffer value = mapCache.get(requestedKey);
                writeBytes(channelHandlerContext, cacheOperation, value);
            }
        } else {
            log.warn("Map Cache Operation [{}] not supported", cacheOperation);
        }
    }

    private void writeResult(final ChannelHandlerContext channelHandlerContext, final CacheOperation cacheOperation, final boolean success) {
        log.debug("Map Cache Operation [{}] Success [{}]", cacheOperation, success);
        final CacheOperationResult cacheOperationResult = new CacheOperationResult(success);
        channelHandlerContext.writeAndFlush(cacheOperationResult);
    }

    private void writeSize(final ChannelHandlerContext channelHandlerContext, final CacheOperation cacheOperation, final int size) {
        final MapSizeResponse mapSizeResponse = new MapSizeResponse(size);
        log.debug("Map Cache Operation [{}] Size [{}]", cacheOperation, size);
        channelHandlerContext.writeAndFlush(mapSizeResponse);
    }

    private void writeBytes(final ChannelHandlerContext channelHandlerContext, final CacheOperation cacheOperation, final ByteBuffer buffer) {
        final byte[] bytes = buffer == null ? null : buffer.array();
        final int length = bytes == null ? 0 : bytes.length;
        final MapValueResponse mapValueResponse = new MapValueResponse(length, bytes);
        log.debug("Map Cache Operation [{}] Length [{}]", cacheOperation, length);
        channelHandlerContext.writeAndFlush(mapValueResponse);
    }

    private void writeMapCacheRecord(final ChannelHandlerContext channelHandlerContext, final CacheOperation cacheOperation, final MapCacheRecord mapCacheRecord) {
        final long revision = mapCacheRecord == null ? REVISION_NOT_FOUND : mapCacheRecord.getRevision();
        final byte[] value = mapCacheRecord == null ? null : mapCacheRecord.getValue().array();
        final int length = value == null ? 0 : value.length;
        final MapValueResponse mapValueResponse = new MapValueResponse(length, value, revision);
        log.debug("Map Cache Operation [{}] Length [{}]", cacheOperation, length);
        channelHandlerContext.writeAndFlush(mapValueResponse);
    }
}
