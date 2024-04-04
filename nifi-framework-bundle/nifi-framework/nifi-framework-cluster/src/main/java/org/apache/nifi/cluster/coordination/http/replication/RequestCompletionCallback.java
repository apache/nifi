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

package org.apache.nifi.cluster.coordination.http.replication;

import java.util.Set;

import org.apache.nifi.cluster.manager.NodeResponse;

/**
 * A callback that can be registered to be called after an HTTP Request is replicated and all nodes'
 * responses have been accounted for (either successfully or not)
 */
public interface RequestCompletionCallback {

    /**
     * Called after all NodeResponse objects have been gathered for the request
     *
     * @param uriPath the path of the request URI
     * @param method the HTTP method of the request
     * @param nodeResponses the NodeResponse for each node that the request was replicated to
     */
    void afterRequest(String uriPath, String method, Set<NodeResponse> nodeResponses);

}
