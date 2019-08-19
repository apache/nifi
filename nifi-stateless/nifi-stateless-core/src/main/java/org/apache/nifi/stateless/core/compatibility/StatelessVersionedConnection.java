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
package org.apache.nifi.stateless.core.compatibility;

import org.apache.nifi.registry.flow.VersionedConnection;

import java.util.Set;

public class StatelessVersionedConnection implements StatelessConnection {

    private final VersionedConnection connection;

    public StatelessVersionedConnection(final VersionedConnection connection) {
        this.connection = connection;
    }

    @Override
    public String getId() {
        return this.connection.getIdentifier();
    }

    @Override
    public Set<String> getSelectedRelationships() {
        return this.connection.getSelectedRelationships();
    }

    @Override
    public StatelessConnectable getSource() {
        return new StatelessConnectableComponent(this.connection.getSource());
    }

    @Override
    public StatelessConnectable getDestination() {
        return new StatelessConnectableComponent(this.connection.getDestination());
    }
}
