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

package org.apache.nifi.cluster.coordination.http;

import java.net.URI;
import java.util.Set;

import org.apache.nifi.cluster.manager.NodeResponse;

/**
 * <p>
 * Maps a set of NodeResponses to a single NodeResponse for a specific REST Endpoint.
 * </p>
 *
 * <p>
 * Implementations of this interface MUST be Thread-Safe.
 * </p>
 */
public interface EndpointResponseMerger {

    /**
     * Indicates whether or not this EndpointResponseMapper can handle mapping responses
     * for the given URI and HTTP Method
     *
     * @param uri the URI of the endpoint
     * @param method the HTTP Method used to interact with the endpoint
     *
     * @return <code>true</code> if the EndpointResponseMapper can handle mapping responses
     *         for the endpoint described by the given URI and HTTP Method
     */
    boolean canHandle(URI uri, String method);

    /**
     * Maps the given Node Responses to a single NodeResponse that is appropriate to return
     * to the client/user
     *
     * @param uri the URI of the REST Endpoint
     * @param method the HTTP Method used to interact with the REST Endpoint
     * @param successfulResponses the responses from nodes that were successful in handling the request
     * @param problematicResponses the responses from nodes that were not successful in handling the request
     * @param clientResponse the response that was chosen to be returned to the client
     *
     * @return a NodeResponse that is appropriate to return to the client/user
     */
    NodeResponse merge(URI uri, String method, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses, NodeResponse clientResponse);

}
