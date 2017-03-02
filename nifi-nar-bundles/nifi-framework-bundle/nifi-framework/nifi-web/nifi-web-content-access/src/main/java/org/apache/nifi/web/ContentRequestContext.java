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
package org.apache.nifi.web;

/**
 * A request for content.
 */
public interface ContentRequestContext {

    /**
     * The URI to the data.
     *
     * @return the uri of the data
     */
    String getDataUri();

    /**
     * If clustered, this is the id of the node the data resides on.
     *
     * @return the the cluster node identifier
     */
    String getClusterNodeId();

    /**
     * The client id for the user making the request.
     *
     * @return the client identifier
     */
    String getClientId();

    /**
     * The proxy chain for the current request, if applicable.
     *
     * Update:
     * This method has been deprecated since the entire proxy
     * chain is able to be rebuilt using the current user if necessary.
     *
     * @return the proxied entities chain
     */
    @Deprecated
    String getProxiedEntitiesChain();
}
