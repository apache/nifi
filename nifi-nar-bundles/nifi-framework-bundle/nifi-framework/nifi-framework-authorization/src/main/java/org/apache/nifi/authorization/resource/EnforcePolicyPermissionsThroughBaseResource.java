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
package org.apache.nifi.authorization.resource;

/**
 * Defers permissions on policies to the policies of the base authorizable. Required because we don't
 * want to change the enforcement of the policies on the authorizable. For example...
 *
 * if a user has permissions to /policies/input-ports/1234 then they have permissions to the following
 *
 * - the policy for /input-ports/1234                -> /policies/input-ports/1234
 * - the policy for /data/input-ports/1234           -> /policies/data/input-ports/1234
 * - the policy for /data-transfers/input-ports/1234 -> /policies/data-transfers/input-ports/1234
 * - the policy for /policies/input-ports/1234       -> /policies/policies/input-ports/1234
 */
public interface EnforcePolicyPermissionsThroughBaseResource {

    /**
     * Returns the base authorizable. Cannot be null.
     *
     * @return base authorizable
     */
    Authorizable getBaseAuthorizable();
}
