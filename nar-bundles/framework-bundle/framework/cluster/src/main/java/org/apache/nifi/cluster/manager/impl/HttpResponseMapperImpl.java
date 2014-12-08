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
package org.apache.nifi.cluster.manager.impl;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.cluster.manager.HttpResponseMapper;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.node.Node;
import org.apache.nifi.cluster.node.Node.Status;
import org.apache.nifi.logging.NiFiLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines the status of nodes based on their HTTP response codes.
 *
 * The algorithm is as follows.
 *
 * If any HTTP responses were 2XX, then disconnect non-2XX responses. This is
 * because 2XX may have changed a node's flow.
 *
 * If no 2XX responses were received, then the node's flow has not changed.
 * Instead of disconnecting everything, we only disconnect the nodes with
 * internal errors, i.e., 5XX responses.
 *
 * @author unattributed
 */
public class HttpResponseMapperImpl implements HttpResponseMapper {

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(HttpResponseMapperImpl.class));

    @Override
    public Map<NodeResponse, Status> map(final URI requestURI, final Set<NodeResponse> nodeResponses) {

        final Map<NodeResponse, Status> result = new HashMap<>();

        // check if any responses were 2XX
        boolean found2xx = false;
        for (final NodeResponse nodeResponse : nodeResponses) {
            if (nodeResponse.is2xx()) {
                found2xx = true;
                break;
            }
        }

        // determine the status of each node 
        for (final NodeResponse nodeResponse : nodeResponses) {

            final Node.Status status;
            if (found2xx) {
                // disconnect nodes with non-2XX responses
                status = nodeResponse.is2xx()
                        ? Node.Status.CONNECTED
                        : Node.Status.DISCONNECTED;
            } else {
                // disconnect nodes with 5XX responses or exception
                status = nodeResponse.is5xx()
                        ? Node.Status.DISCONNECTED
                        : Node.Status.CONNECTED;
            }

            result.put(nodeResponse, status);
        }

        return result;
    }

}
