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

package org.apache.nifi.controller.queue.clustered.server;

import java.util.Collection;

public interface LoadBalanceAuthorizer {
    /**
     * Checks if any of the given identities is allowed to load balance data. If so, the identity that has been
     * permitted is returned. If not, a NotAuthorizedException is thrown.
     *
     * @param clientIdentities the collection of identities to check
     * @return the identity that is authorized, or null if the given collection of identities is null
     * @throws NotAuthorizedException if none of the given identities is authorized to load balance data
     */
    String authorize(Collection<String> clientIdentities) throws NotAuthorizedException;
}
