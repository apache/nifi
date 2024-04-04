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
package org.apache.nifi.event.transport.netty.codec;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.ssl.SslHandler;
import org.apache.nifi.event.transport.SslSessionStatus;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;

/**
 * Message Decoder for bytes received from Socket Channels
 */
public class SocketByteArrayMessageDecoder extends MessageToMessageDecoder<byte[]> {
    private static final Logger logger = LoggerFactory.getLogger(SocketByteArrayMessageDecoder.class);

    /**
     * Decode bytes to Byte Array Message with remote address from Channel.remoteAddress()
     *
     * @param channelHandlerContext Channel Handler Context
     * @param bytes Message Bytes
     * @param decoded Decoded Messages
     */
    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final byte[] bytes, final List<Object> decoded) {
        final InetSocketAddress remoteAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
        final String address = remoteAddress.getHostString();

        final SslSessionStatus sslSessionStatus = getSslSessionStatus(channelHandlerContext);
        final ByteArrayMessage message = new ByteArrayMessage(bytes, address, sslSessionStatus);

        decoded.add(message);
    }

    private SslSessionStatus getSslSessionStatus(final ChannelHandlerContext channelHandlerContext) {
        SslHandler sslHandler = null;
        for (final Map.Entry<String, ChannelHandler> entry : channelHandlerContext.channel().pipeline()) {
            final ChannelHandler channelHandler = entry.getValue();
            if (channelHandler instanceof SslHandler) {
                sslHandler = (SslHandler) channelHandler;
                break;
            }
        }
        return sslHandler == null ? null : createSslSessionStatusFromSslHandler(sslHandler);
    }

    private SslSessionStatus createSslSessionStatusFromSslHandler(final SslHandler sslHandler) {
        final SSLSession sslSession = sslHandler.engine().getSession();
        SslSessionStatus sslSessionStatus = null;
        try {
            final Certificate[] certificates = sslSession.getPeerCertificates();
            if (certificates.length > 0) {
                final X509Certificate certificate = (X509Certificate) certificates[0];
                final X500Principal subject = certificate.getSubjectX500Principal();
                final X500Principal issuer = certificate.getIssuerX500Principal();
                sslSessionStatus = new SslSessionStatus(subject, issuer);
            }
        } catch (final SSLPeerUnverifiedException peerUnverifiedException) {
            logger.debug("Peer Unverified", peerUnverifiedException);
        }

        return sslSessionStatus;
    }
}
