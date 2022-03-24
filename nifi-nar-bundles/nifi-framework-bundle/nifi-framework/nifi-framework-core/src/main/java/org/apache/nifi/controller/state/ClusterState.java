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

package org.apache.nifi.controller.state;

import java.util.Set;

public interface ClusterState {
    /**
     * @return <code>true</code> if this instance of NiFi is connected to a cluster, <code>false</code> if the node is disconnected
     */
    boolean isConnected();

    /**
     * @return the identifier that is used to identify this node in the cluster
     */
    String getNodeIdentifier();

    /**
     * @return a Set of {@link NodeDescription} objects that can be used to determine which other nodes are in the same cluster. This
     *         Set will not be <code>null</code> but will be empty if the node is not connected to a cluster
     */
    Set<NodeDescription> getNodes();
}
