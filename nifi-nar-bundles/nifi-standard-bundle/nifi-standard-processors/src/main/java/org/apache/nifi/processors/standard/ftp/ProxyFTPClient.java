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
package org.apache.nifi.processors.standard.ftp;

import org.apache.commons.net.ftp.FTPClient;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Proxy extension of FTP Client supporting connections using unresolved addresses
 */
public class ProxyFTPClient extends FTPClient {
    /**
     * Proxy FTP Client constructor with required Socket Factory configured for proxy access
     *
     * @param socketFactory Socket Factory
     */
    public ProxyFTPClient(final SocketFactory socketFactory) {
        setSocketFactory(Objects.requireNonNull(socketFactory, "Socket Factory required"));
    }

    /**
     * Connect using host and port without attempting DNS resolution using InetAddress.getByName()
     *
     * @param host FTP host
     * @param port FTP port number
     * @throws IOException Thrown when failing to complete a socket connection
     */
    @Override
    public void connect(final String host, final int port) throws IOException {
        final InetSocketAddress socketAddress = new InetSocketAddress(host, port);

        _socket_ = _socketFactory_.createSocket();

        _socket_.connect(socketAddress, connectTimeout);
        _connectAction_();
    }
}
