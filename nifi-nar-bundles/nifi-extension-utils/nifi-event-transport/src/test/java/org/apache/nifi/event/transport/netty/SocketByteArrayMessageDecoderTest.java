/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.event.transport.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SslHandler;
import org.apache.nifi.event.transport.SslSessionStatus;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.codec.SocketByteArrayMessageDecoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class SocketByteArrayMessageDecoderTest extends SocketByteArrayMessageDecoder {

    private InetSocketAddress inetSocketAddress;
    private X509Certificate certificate;
    @Mock
    private ChannelHandlerContext channelHandlerContext;
    @Mock
    private Channel channel;
    @Mock
    private ChannelPipeline channelPipeline;
    @Mock
    private Iterator<Map.Entry<String, ChannelHandler>> iterator;
    @Mock
    private Map.Entry<String, ChannelHandler> pipelineEntry;
    @Mock
    private SslHandler sslHandler;
    @Mock
    private ChannelOutboundHandlerAdapter nonSslHandler;
    @Mock
    private SSLEngine sslEngine;
    @Mock
    private SSLSession sslSession;

    @BeforeEach
    public void init() {
        inetSocketAddress = new InetSocketAddress("localhost", 21000);
        Mockito.when(channelHandlerContext.channel()).thenReturn(channel);
        Mockito.when(channel.remoteAddress()).thenReturn(inetSocketAddress);
        Mockito.when(channel.pipeline()).thenReturn(channelPipeline);
        Mockito.when(channelPipeline.iterator()).thenReturn(iterator);
        Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(iterator.next()).thenReturn(pipelineEntry);
    }

    @Test
    public void testDecodeMessageSsl() throws Exception {
        String expectedSubjectDN = "CN=Subject,O=Subject Organization";
        String expectedIssuerDN = "CN=Issuer,O=Issuer Organization";

        initSsl();

        List<Object> decoded = new ArrayList<>(1);
        decode(channelHandlerContext, "test".getBytes(), decoded);

        X500Principal actualSubject = ((ByteArrayMessage)decoded.get(0)).getSslSessionStatus().getSubject();
        X500Principal actualIssuer = ((ByteArrayMessage)decoded.get(0)).getSslSessionStatus().getIssuer();
        assertEquals(expectedSubjectDN, actualSubject.getName());
        assertEquals(expectedIssuerDN, actualIssuer.getName());
    }

    @Test
    public void testDecodeMessageNoSsl() {
        initNoSslHandler();

        List<Object> decoded = new ArrayList<>(1);
        decode(channelHandlerContext, "test".getBytes(), decoded);

        SslSessionStatus sslSessionStatus = ((ByteArrayMessage)decoded.get(0)).getSslSessionStatus();
        assertNull(sslSessionStatus);
    }

    @Test
    public void testDecodeMessageNoCertificates() throws Exception {
        initSslWithNoCertificates();

        List<Object> decoded = new ArrayList<>(1);
        decode(channelHandlerContext, "test".getBytes(), decoded);

        SslSessionStatus sslSessionStatus = ((ByteArrayMessage)decoded.get(0)).getSslSessionStatus();
        assertNull(sslSessionStatus);
    }

    @Test
    public void testDecodeMessagePeerUnverified() throws Exception {
        initSslPeerUnverified();

        List<Object> decoded = new ArrayList<>(1);
        decode(channelHandlerContext, "test".getBytes(), decoded);

        SslSessionStatus sslSessionStatus = ((ByteArrayMessage)decoded.get(0)).getSslSessionStatus();
        assertNull(sslSessionStatus);
    }

    private void initSsl() throws Exception {
        initSslEngine();
        try (InputStream inStream = new FileInputStream("src/test/resources/netty/client_certificate.cer")) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            certificate = (X509Certificate)cf.generateCertificate(inStream);
        }
        Mockito.when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{certificate});
    }

    private void initNoSslHandler() {
        Mockito.when(pipelineEntry.getValue()).thenReturn(nonSslHandler);
    }

    private void initSslWithNoCertificates() throws Exception {
        initSslEngine();
        Mockito.when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{});
    }

    private void initSslPeerUnverified() throws Exception {
        initSslEngine();
        Mockito.when(sslSession.getPeerCertificates()).thenThrow(new SSLPeerUnverifiedException("Peer unverified"));
    }

    private void initSslEngine() {
        Mockito.when(pipelineEntry.getValue()).thenReturn(sslHandler);
        Mockito.when(sslHandler.engine()).thenReturn(sslEngine);
        Mockito.when(sslEngine.getSession()).thenReturn(sslSession);
    }
}
