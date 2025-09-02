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

package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.components.connector.components.ConnectionFacade;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flow.VersionedConnection;

public class StandaloneConnectionFacade implements ConnectionFacade {
    private final Connection connection;
    private final VersionedConnection versionedConnection;

    public StandaloneConnectionFacade(final Connection connection, final VersionedConnection versionedConnection) {
        this.connection = connection;
        this.versionedConnection = versionedConnection;
    }

    @Override
    public VersionedConnection getDefinition() {
        return versionedConnection;
    }

    @Override
    public QueueSize getQueueSize() {
        return connection.getFlowFileQueue().size();
    }

    @Override
    public void purge() {
        // TODO: Require arguments here
        connection.getFlowFileQueue().dropFlowFiles("User requested purge", "User");
    }
}
