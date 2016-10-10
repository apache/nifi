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
import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import org.apache.nifi.util.NiFiProperties;

/**
 * Implements a server socket factory for creating secure server sockets based
 * on the application's configuration properties. If the properties are
 * configured for SSL (one-way or two-way), then a SSLServerSocketFactory is
 * created and used based on those properties. Otherwise, Java's default
 * SSLServerSocketFactory is used. Specifically,
 * SSLContext.getDefault().getServerSocketFactory().
 */
public class SslServerSocketFactory extends SSLServerSocketFactory {

    private SSLServerSocketFactory sslServerSocketFactory;

    public SslServerSocketFactory(final NiFiProperties nifiProperties) {
        final SSLContext sslCtx = SslContextFactory.createSslContext(nifiProperties);
        if (sslCtx == null) {
            try {
                sslServerSocketFactory = SSLContext.getDefault().getServerSocketFactory();
            } catch (final NoSuchAlgorithmException nsae) {
                throw new SslServerSocketFactoryCreationException(nsae);
            }
        } else {
            sslServerSocketFactory = sslCtx.getServerSocketFactory();
        }
    }

    @Override
    public ServerSocket createServerSocket(int i, int i1, InetAddress ia) throws IOException {
        return sslServerSocketFactory.createServerSocket(i, i1, ia);
    }

    @Override
    public ServerSocket createServerSocket(int i, int i1) throws IOException {
        return sslServerSocketFactory.createServerSocket(i, i1);
    }

    @Override
    public ServerSocket createServerSocket(int i) throws IOException {
        return sslServerSocketFactory.createServerSocket(i);
    }

    @Override
    public ServerSocket createServerSocket() throws IOException {
        return sslServerSocketFactory.createServerSocket();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return sslServerSocketFactory.getSupportedCipherSuites();
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return sslServerSocketFactory.getDefaultCipherSuites();
    }

}
