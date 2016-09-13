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
package org.apache.nifi.framework.security.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.apache.nifi.util.NiFiProperties;

/**
 * Implements a socket factory for creating secure sockets based on the
 * application's configuration properties. If the properties are configured for
 * SSL (one-way or two-way), then a SSLSocketFactory is created and used based
 * on those properties. Otherwise, Java's default SSLSocketFactory is used.
 * Specifically, SSLContext.getDefault().getSocketFactory().
 */
public class SslSocketFactory extends SSLSocketFactory {

    private final SSLSocketFactory sslSocketFactory;

    public SslSocketFactory(final NiFiProperties nifiProperties) {
        final SSLContext sslCtx = SslContextFactory.createSslContext(nifiProperties);
        if (sslCtx == null) {
            try {
                sslSocketFactory = SSLContext.getDefault().getSocketFactory();
            } catch (final NoSuchAlgorithmException nsae) {
                throw new SslSocketFactoryCreationException(nsae);
            }
        } else {
            sslSocketFactory = sslCtx.getSocketFactory();
        }
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return sslSocketFactory.getSupportedCipherSuites();
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return sslSocketFactory.getDefaultCipherSuites();
    }

    @Override
    public Socket createSocket(Socket socket, String string, int i, boolean bln) throws IOException {
        return sslSocketFactory.createSocket(socket, string, i, bln);
    }

    @Override
    public Socket createSocket(InetAddress ia, int i, InetAddress ia1, int i1) throws IOException {
        return sslSocketFactory.createSocket(ia, i, ia1, i1);
    }

    @Override
    public Socket createSocket(InetAddress ia, int i) throws IOException {
        return sslSocketFactory.createSocket(ia, i);
    }

    @Override
    public Socket createSocket(String string, int i, InetAddress ia, int i1) throws IOException, UnknownHostException {
        return sslSocketFactory.createSocket(string, i, ia, i1);
    }

    @Override
    public Socket createSocket(String string, int i) throws IOException, UnknownHostException {
        return sslSocketFactory.createSocket(string, i);
    }

    @Override
    public Socket createSocket() throws IOException {
        return sslSocketFactory.createSocket();
    }

}
