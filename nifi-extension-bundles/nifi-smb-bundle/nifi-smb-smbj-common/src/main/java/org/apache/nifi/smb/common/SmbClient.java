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
package org.apache.nifi.smb.common;

import com.hierynomus.mssmb2.messages.SMB2Echo;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.event.ConnectionClosed;
import com.hierynomus.smbj.event.SMBEventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Extends {@link com.hierynomus.smbj.SMBClient} with connection health check.
 * <br/>
 * Workaround to https://github.com/hierynomus/smbj/issues/796.
 * <br/><br/>
 * Health check method:
 * <ul>
 *   <li>get connection from the parent class</li>
 *   <li>if it is a newly created connection, then return it</li>
 *   <li>if it is an old connection, send ECHO message to the server
 *     <ul>
 *       <li>if ECHO succeeds, return the connection</li>
 *       <li>if ECHO fails, unregister the connection, get connection again (which creates a new one) and return it</li>
 *     </ul>
 *   </li>
 * </ul>
 */
class SmbClient extends SMBClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmbClient.class);

    private SMBEventBus bus;

    private SmbClient(final SmbConfig config, final SMBEventBus bus) {
        super(config, bus);
    }

    static SmbClient create(final SmbConfig config) {
        final SMBEventBus bus = new SMBEventBus();

        final SmbClient client = new SmbClient(config, bus);

        client.bus = bus;

        return client;
    }

    public Connection connect(final String hostname) throws IOException {
        return connect(hostname, DEFAULT_PORT);
    }

    public synchronized Connection connect(final String hostname, final int port) throws IOException {
        final Connection connection = super.connect(hostname, port);

        try {
            // SMB2 ECHO message can only be sent if this is not a new connection (and health check is only needed in this case)
            if (!connection.release()) {
                connection.send(new SMB2Echo(connection.getNegotiatedProtocol().getDialect())).get(10, TimeUnit.SECONDS);
            }

            // set lease counter back
            connection.lease();

            return connection;
        } catch (Exception e) {
            LOGGER.info("Stale connection found, unregistering it and creating a new one");
            bus.publish(new ConnectionClosed(hostname, port));
        }

        return super.connect(hostname, port);
    }
}
