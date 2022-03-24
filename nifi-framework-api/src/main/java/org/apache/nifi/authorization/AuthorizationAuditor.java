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
package org.apache.nifi.authorization;

public interface AuthorizationAuditor {

    /**
     * Audits an authorization request. Will be invoked for any Approved or Denied results. ResourceNotFound
     * will either re-attempt authorization using a parent resource or will generate a failure result and
     * audit that.
     *
     * @param request the request for authorization
     * @param result the authorization result
     */
    void auditAccessAttempt(final AuthorizationRequest request, final AuthorizationResult result);
}
