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

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.user.NiFiUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorizable for a component that can be scheduled by operators.
 */
public class OperationAuthorizable implements Authorizable, EnforcePolicyPermissionsThroughBaseResource {
    private static Logger logger = LoggerFactory.getLogger(OperationAuthorizable.class);
    private final Authorizable baseAuthorizable;

    public OperationAuthorizable(final Authorizable baseAuthorizable) {
        this.baseAuthorizable = baseAuthorizable;
    }

    @Override
    public Authorizable getParentAuthorizable() {
        // Need to return parent operation authorizable. E.g. /operation/processor/xxxx -> /operation/process-group/yyyy -> /operation/process-group/root
        if (baseAuthorizable.getParentAuthorizable() == null) {
            return null;
        } else {
            return new OperationAuthorizable(baseAuthorizable.getParentAuthorizable());
        }
    }

    @Override
    public Authorizable getBaseAuthorizable() {
        return baseAuthorizable;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getOperationResource(baseAuthorizable.getResource());
    }

    /**
     * <p>Authorize the request operation action with the resource using base authorizable and operation authorizable combination.</p>
     *
     * <p>This method authorizes the request with the base authorizable first with WRITE action. If the request is allowed, then finish authorization.
     * If the base authorizable denies the request, then it checks if the user has WRITE permission for '/operation/{componentType}/{id}'.</p>
     */
    public static void authorizeOperation(final Authorizable baseAuthorizable, final Authorizer authorizer, final NiFiUser user) {
        try {
            baseAuthorizable.authorize(authorizer, RequestAction.WRITE, user);
        } catch (AccessDeniedException e) {
            logger.debug("Authorization failed with {}. Try authorizing with OperationAuthorizable.", baseAuthorizable, e);
            // Always use WRITE action for operation.
            new OperationAuthorizable(baseAuthorizable).authorize(authorizer, RequestAction.WRITE, user);
        }

    }

    /**
     * Check if the request is authorized.
     *
     * @return True if the WRITE request is allowed by the base authorizable, or the user has WRITE permission for '/operation/{componentType}/id'.
     */
    public static boolean isOperationAuthorized(final Authorizable baseAuthorizable, final Authorizer authorizer, final NiFiUser user) {
        return baseAuthorizable.isAuthorized(authorizer, RequestAction.WRITE, user)
                || new OperationAuthorizable(baseAuthorizable).isAuthorized(authorizer, RequestAction.WRITE, user);
    }

}
