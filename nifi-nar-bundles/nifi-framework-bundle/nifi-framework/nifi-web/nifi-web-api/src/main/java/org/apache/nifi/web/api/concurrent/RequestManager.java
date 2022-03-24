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

package org.apache.nifi.web.api.concurrent;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.web.ResourceNotFoundException;

import java.util.function.Consumer;

public interface RequestManager<R, T> {

    /**
     * Submits a request to be performed in the background
     *
     * @param requestType the type of request to submit. This value can be anything and is used along with the id in order to create
     *            a composite key for the request so that different request types may easily be managed by the RequestManager.
     * @param id the ID of the request
     * @param request the request
     * @param task the task that should be performed in the background
     *
     * @throws IllegalArgumentException if a request already exists with the given ID
     * @throws NullPointerException if any argument is null
     */
    void submitRequest(String requestType, String id, AsynchronousWebRequest<R, T> request, Consumer<AsynchronousWebRequest<R, T>> task);

    /**
     * Retrieves the request with the given ID
     *
     * @param requestType the type of the request being retrieved
     * @param id the ID of the request
     * @param user the user who is retrieving the request
     * @return the request with the given ID
     *
     * @throws ResourceNotFoundException if no request can be found with the given ID
     * @throws IllegalArgumentException if the user given is not the user that submitted the request
     * @throws NullPointerException if either the ID or the user is null
     */
    AsynchronousWebRequest<R, T> getRequest(String requestType, String id, NiFiUser user);

    /**
     * Removes the request with the given ID
     *
     * @param requestType the type of the request being removed
     * @param id the ID of the request
     * @param user the user who is retrieving the request
     * @return the request with the given ID
     *
     * @throws ResourceNotFoundException if no request can be found with the given ID
     * @throws IllegalArgumentException if the user given is not the user that submitted the request
     * @throws IllegalStateException if the request with the given ID is not yet complete
     * @throws NullPointerException if either the ID or the user is null
     */
    AsynchronousWebRequest<R, T> removeRequest(String requestType, String id, NiFiUser user);

}
