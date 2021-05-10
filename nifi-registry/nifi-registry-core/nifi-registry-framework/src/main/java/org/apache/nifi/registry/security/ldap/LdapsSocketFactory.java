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
package org.apache.nifi.registry.security.ldap;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * SSLSocketFactory used when connecting to a Directory Server over LDAPS.
 */
public class LdapsSocketFactory extends SSLSocketFactory {

    // singleton
    private static LdapsSocketFactory instance;

    // delegate
    private SSLSocketFactory delegate;

    /**
     * Initializes the LdapsSocketFactory with the specified SSLSocketFactory. The specified
     * socket factory will be used as a delegate for all subsequent instances of this class.
     *
     * @param sslSocketFactory delegate socket factory
     */
    public static void initialize(final SSLSocketFactory sslSocketFactory) {
        instance = new LdapsSocketFactory(sslSocketFactory);
    }

    /**
     * Gets the LdapsSocketFactory that was previously initialized.
     *
      * @return socket factory
     */
    public static SocketFactory getDefault() {
        return instance;
    }

    /**
     * Creates a new LdapsSocketFactory.
     *
     * @param sslSocketFactory delegate socket factory
     */
    private LdapsSocketFactory(final SSLSocketFactory sslSocketFactory) {
        delegate = sslSocketFactory;
    }

    // delegate methods

    @Override
    public String[] getSupportedCipherSuites() {
        return delegate.getSupportedCipherSuites();
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return delegate.getDefaultCipherSuites();
    }

    @Override
    public Socket createSocket(Socket socket, String string, int i, boolean bln) throws IOException {
        return delegate.createSocket(socket, string, i, bln);
    }

    @Override
    public Socket createSocket(InetAddress ia, int i, InetAddress ia1, int i1) throws IOException {
        return delegate.createSocket(ia, i, ia1, i1);
    }

    @Override
    public Socket createSocket(InetAddress ia, int i) throws IOException {
        return delegate.createSocket(ia, i);
    }

    @Override
    public Socket createSocket(String string, int i, InetAddress ia, int i1) throws IOException, UnknownHostException {
        return delegate.createSocket(string, i, ia, i1);
    }

    @Override
    public Socket createSocket(String string, int i) throws IOException, UnknownHostException {
        return delegate.createSocket(string, i);
    }

    @Override
    public Socket createSocket() throws IOException {
        return delegate.createSocket();
    }
}
