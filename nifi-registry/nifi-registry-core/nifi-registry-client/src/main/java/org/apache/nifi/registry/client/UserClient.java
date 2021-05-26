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
package org.apache.nifi.registry.client;

import org.apache.nifi.registry.authorization.CurrentUser;

import java.io.IOException;

public interface UserClient {

    /**
     * Obtains the access status of the current user.
     *
     * If the UserClient was obtained with proxied entities, then the access status should represent the status
     * of the last identity in the chain.
     *
     * If the UserClient was obtained without proxied entities, then it would represent the identity of the certificate
     * in the keystore used by the client.
     *
     * If the registry is not in secure mode, the anonymous identity is expected to be returned along with a flag indicating
     * the user is anonymous.
     *
     * @return the access status of the current user
     * @throws NiFiRegistryException if the proxying user is not a valid proxy or identity claim is otherwise invalid
     */
    CurrentUser getAccessStatus() throws NiFiRegistryException, IOException;

}
