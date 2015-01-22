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
/**
 * <p>
 * The NiFi REST API allows clients to obtain and update configuration and
 * status information pertaining to an instance of NiFi. The links below detail
 * each resource. Follow the link to get more information about the resource
 * including the supported HTTP methods and the expected parameters.</p>
 *
 * <p>
 * Additionally, the documentation for each resource will describe what type of
 * data should be returned from a successful invocation. However, if the request
 * is not successful one of the follow status codes should be returned:</p>
 *
 * <ul>
 * <li>400 (Bad Request) - A 400 status code will be returned when NiFi is
 * unable to complete the request because it was invalid. The request should not
 * be retried without modification.</li>
 * <li>401 (Unathorized) - A 401 status code indicates that the user is not
 * known to this NiFi instance. The user may submit an account request.</li>
 * <li>403 (Forbidden) - A 403 status code indicates that the user is known to
 * this NiFi instance and they do not have authority to perform the requested
 * action.</li>
 * <li>404 (Not Found) - A 404 status code will be returned when the desired
 * resource does not exist.</li>
 * <li>409 (Conflict) - NiFi employs an optimistic locking strategy where the
 * client must include a revision in their request when performing an update. If
 * the specified revision does not match the current base revision a 409 status
 * code is returned. Additionally, a 409 is used when the state of the system
 * does not allow for the request at that time. This same request may be
 * successful later if the system is in a different state (e.g. cannot delete a
 * processor because it is currently running).</li>
 * <li>500 (Internal Server Error) - A 500 status code indicates that an
 * unexpected error has occurred.</li>
 * </ul>
 *
 * <p>
 * Most unsuccessful requests will include a description of the problem in the
 * entity body of the response.</p>
 *
 * <p>
 * The context path for the REST API is /nifi-api</p>
 */
package org.apache.nifi.web.api;
