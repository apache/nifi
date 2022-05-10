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
import org.apache.nifi.event.transport.SslInfo;
import org.apache.nifi.event.transport.message.ByteArrayMessage;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * Message Decoder for bytes received from Socket Channels
 */
public class SocketByteArrayMessageDecoder extends MessageToMessageDecoder<byte[]> {
    private static String SSL_HANDLER = "SslHandler#0";
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

        ByteArrayMessage message = getMessageWithSslInfo(channelHandlerContext, bytes, address);
        if (message == null) {
            message = new ByteArrayMessage(bytes, address);
        }
        decoded.add(message);
    }

    private ByteArrayMessage getMessageWithSslInfo(final ChannelHandlerContext channelHandlerContext, final byte[] bytes, final String address) {
        final ChannelHandler sslHandler = channelHandlerContext.channel().pipeline().get(SSL_HANDLER);
        if (sslHandler != null) {
            final SSLSession sslSession = ((SslHandler)sslHandler).engine().getSession();
            try {
                final Certificate[] certificates = sslSession.getPeerCertificates();
                if (certificates.length > 0) {
                    final X509Certificate certificate = (X509Certificate) certificates[0];
                    final String subjectDN = certificate.getSubjectDN().toString();
                    final String issuerDN = certificate.getIssuerDN().toString();
                    return new ByteArrayMessage(bytes, address, new SslInfo(subjectDN, issuerDN));
                }
            } catch (SSLPeerUnverifiedException peerUnverifiedException) {
                return null;
            }
        }
        return null;
    }
}
