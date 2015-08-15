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
package org.apache.nifi.http;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nifi.controller.ControllerService;

/**
 * <p>
 * An interface that provides the capability of receiving an HTTP servlet request in one component and responding to that request in another component.
 * </p>
 *
 * <p>
 * The intended flow is for the component receiving the HTTP request to register the request, response, and AsyncContext with a particular identifier via the
 * {@link #register(String, HttpServletRequest, HttpServletResponse, AsyncContext)} method. Another component is then able to obtain the response by providing that identifier to the
 * {@link #getResponse(String)} method. After writing to the HttpServletResponse, the transaction is to then be completed via the {@link #complete(String)} method.
 * </p>
 */
public interface HttpContextMap extends ControllerService {

    /**
     * Registers an HttpServletRequest, HttpServletResponse, and the AsyncContext for a given identifier
     *
     * @param identifier identifier
     * @param request request
     * @param response response
     * @param context context
     *
     * @return true if register is successful, false if the context map is too full because too many requests have already been received and not processed
     *
     * @throws IllegalStateException if the identifier is already registered
     */
    boolean register(String identifier, HttpServletRequest request, HttpServletResponse response, AsyncContext context);

    /**
     * Retrieves the HttpServletResponse for the given identifier, if it exists
     *
     * @param identifier identifier
     * @return the HttpServletResponse for the given identifier, or {@code null} if it does not exist
     */
    HttpServletResponse getResponse(String identifier);

    /**
     * Marks the HTTP request/response for the given identifier as complete
     *
     * @param identifier identifier
     *
     * @throws IllegalStateException if the identifier is not registered to a valid AsyncContext
     */
    void complete(String identifier);

}
