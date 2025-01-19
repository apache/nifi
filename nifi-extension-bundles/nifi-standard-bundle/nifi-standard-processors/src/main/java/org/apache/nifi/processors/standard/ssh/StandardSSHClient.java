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
package org.apache.nifi.processors.standard.ssh;

import com.exceptionfactory.socketbroker.BrokeredSocketFactory;
import net.schmizz.sshj.Config;
import net.schmizz.sshj.SSHClient;
import org.apache.nifi.processors.standard.socket.ProxySocketFactory;

import javax.net.SocketFactory;
import java.net.InetSocketAddress;

/**
 * Standard extension of SSHJ SSHClient supporting unresolved Socket Addresses for control over proxy behavior
 */
public class StandardSSHClient extends SSHClient {

    public StandardSSHClient(final Config config) {
        super(config);
    }

    /**
     * Create InetSocketAddress to based on proxy configuration
     *
     * @param hostname Hostname or Internet Protocol address
     * @param port TCP port number
     * @return Socket Address resolved or unresolved based on proxy configuration
     */
    @Override
    protected InetSocketAddress makeInetSocketAddress(final String hostname, final int port) {
        final SocketFactory socketFactory = getSocketFactory();
        final boolean proxyConfigured = socketFactory instanceof ProxySocketFactory || socketFactory instanceof BrokeredSocketFactory;

        final InetSocketAddress socketAddress;
        if (proxyConfigured) {
            socketAddress = InetSocketAddress.createUnresolved(hostname, port);
        } else {
            socketAddress = new InetSocketAddress(hostname, port);
        }
        return socketAddress;
    }
}
