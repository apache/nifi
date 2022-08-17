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
import org.apache.nifi.distributed.cache.operations.SetOperation;
import org.apache.nifi.distributed.cache.server.protocol.CacheOperationResult;
import org.apache.nifi.distributed.cache.server.protocol.CacheRequest;
import org.apache.nifi.distributed.cache.server.set.SetCache;
import org.apache.nifi.distributed.cache.server.set.SetCacheResult;
import org.apache.nifi.logging.ComponentLog;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Handler for Set Cache Request operations interacts with the Set Cache and writes Results
 */
@ChannelHandler.Sharable
public class SetCacheRequestHandler extends SimpleChannelInboundHandler<CacheRequest> {
    private final ComponentLog log;

    private final SetCache setCache;

    public SetCacheRequestHandler(
            final ComponentLog log,
            final SetCache setCache
    ) {
        this.log = Objects.requireNonNull(log, "Component Log required");
        this.setCache = Objects.requireNonNull(setCache, "Set Cache required");
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final CacheRequest cacheRequest) throws Exception {
        final CacheOperation cacheOperation = cacheRequest.getCacheOperation();
        final ByteBuffer body = ByteBuffer.wrap(cacheRequest.getBody());

        final CacheOperationResult result;
        if (SetOperation.ADD_IF_ABSENT == cacheOperation) {
            result = getCacheOperationResult(setCache.addIfAbsent(body));
        } else if (SetOperation.CONTAINS == cacheOperation) {
            result = getCacheOperationResult(setCache.contains(body));
        } else if (SetOperation.REMOVE == cacheOperation) {
            result = getCacheOperationResult(setCache.remove(body));
        } else {
            result = null;
        }

        if (SetOperation.CLOSE == cacheOperation) {
            log.debug("Set Cache Operation [{}] received", cacheOperation);
            channelHandlerContext.close();
        } else if (result == null) {
            log.warn("Set Cache Operation [{}] not supported", cacheOperation);
        } else {
            log.debug("Set Cache Operation [{}] Success [{}]", cacheOperation, result.isSuccess());
            channelHandlerContext.writeAndFlush(result);
        }
    }

    private CacheOperationResult getCacheOperationResult(final SetCacheResult setCacheResult) {
        return new CacheOperationResult(setCacheResult.getResult());
    }
}
