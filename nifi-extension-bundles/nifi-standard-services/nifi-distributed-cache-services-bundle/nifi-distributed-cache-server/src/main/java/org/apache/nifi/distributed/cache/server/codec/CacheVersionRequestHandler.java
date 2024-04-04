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
import org.apache.nifi.distributed.cache.protocol.ProtocolHandshake;
import org.apache.nifi.distributed.cache.server.protocol.CacheVersionRequest;
import org.apache.nifi.distributed.cache.server.protocol.CacheVersionResponse;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.remote.VersionNegotiator;

import java.util.Map;
import java.util.Objects;

/**
 * Handler for Cache Version Requests responsible for negotiating supported version
 */
public class CacheVersionRequestHandler extends SimpleChannelInboundHandler<CacheVersionRequest> {
    private final ComponentLog log;

    private final VersionNegotiator versionNegotiator;

    public CacheVersionRequestHandler(
            final ComponentLog log,
            final VersionNegotiator versionNegotiator
    ) {
        this.log = Objects.requireNonNull(log, "Component Log required");
        this.versionNegotiator = Objects.requireNonNull(versionNegotiator, "Version Negotiator required");
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final CacheVersionRequest cacheVersionRequest) {
        for (final Map.Entry<String, ChannelHandler> entry : channelHandlerContext.channel().pipeline()) {
            final ChannelHandler channelHandler = entry.getValue();
            if (channelHandler instanceof CacheRequestDecoder) {
                final CacheRequestDecoder cacheRequestDecoder = (CacheRequestDecoder) channelHandler;

                final int requestedVersion = cacheVersionRequest.getVersion();
                final CacheVersionResponse cacheVersionResponse = handleVersion(cacheRequestDecoder, requestedVersion);
                channelHandlerContext.writeAndFlush(cacheVersionResponse);
            }
        }
    }

    private CacheVersionResponse handleVersion(final CacheRequestDecoder cacheRequestDecoder, final int requestedVersion) {
        final CacheVersionResponse cacheVersionResponse;

        if (versionNegotiator.isVersionSupported(requestedVersion)) {
            log.debug("Cache Version Supported [{}]", requestedVersion);
            cacheRequestDecoder.setProtocolVersion(requestedVersion);
            cacheVersionResponse = new CacheVersionResponse(ProtocolHandshake.RESOURCE_OK, requestedVersion);
        } else {
            final Integer preferredVersion = versionNegotiator.getPreferredVersion(requestedVersion);
            if (preferredVersion == null) {
                log.debug("Cache Version Rejected [{}]", requestedVersion);
                cacheVersionResponse = new CacheVersionResponse(ProtocolHandshake.ABORT, requestedVersion);
            } else {
                log.debug("Cache Version Rejected [{}] Preferred [{}]", requestedVersion, preferredVersion);
                cacheVersionResponse = new CacheVersionResponse(ProtocolHandshake.DIFFERENT_RESOURCE_VERSION, preferredVersion);
            }
        }

        return cacheVersionResponse;
    }
}
