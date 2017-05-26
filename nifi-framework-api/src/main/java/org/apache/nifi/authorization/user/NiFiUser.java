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

package org.apache.nifi.authorization.user;

import java.util.Set;

/**
 * A representation of a NiFi user that has logged into the application
 */
public interface NiFiUser {

    /**
     * @return the unique identity of this user
     */
    String getIdentity();

    /**
     * @return the groups that this user belongs to if this nifi is configured to load user groups, null otherwise.
     */
    Set<String> getGroups();

    /**
     * @return the next user in the proxied entities chain, or <code>null</code> if no more users exist in the chain.
     */
    NiFiUser getChain();

    /**
     * @return <code>true</code> if the user is the unauthenticated Anonymous user
     */
    boolean isAnonymous();

    /**
     * @return the address of the client that made the request which created this user
     */
    String getClientAddress();

}
