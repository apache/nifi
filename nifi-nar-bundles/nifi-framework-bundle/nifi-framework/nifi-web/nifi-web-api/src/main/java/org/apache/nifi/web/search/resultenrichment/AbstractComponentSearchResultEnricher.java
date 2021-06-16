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
package org.apache.nifi.web.search.resultenrichment;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.api.dto.search.SearchResultGroupDTO;

abstract class AbstractComponentSearchResultEnricher implements ComponentSearchResultEnricher {
    protected final ProcessGroup processGroup;
    protected final NiFiUser user;
    protected final Authorizer authorizer;

    AbstractComponentSearchResultEnricher(final ProcessGroup processGroup, final NiFiUser user, final Authorizer authorizer) {
        this.processGroup = processGroup;
        this.user = user;
        this.authorizer = authorizer;
    }

    /**
     * Builds the nearest versioned parent result group for a given user.
     *
     * @param group The containing group
     * @param user The current NiFi user
     * @return Versioned parent group
     */
    protected SearchResultGroupDTO buildVersionedGroup(final ProcessGroup group, final NiFiUser user) {
        if (group == null) {
            return null;
        }

        ProcessGroup current = group;

        // search for a versioned group by traversing the group tree up to the root
        while (!current.isRootGroup()) {
            if (current.getVersionControlInformation() != null) {
                return buildResultGroup(current, user);
            }

            current = current.getParent();
        }

        // traversed all the way to the root
        return null;
    }

    /**
     * Builds result group for a given user.
     *
     * @param group The containing group
     * @param user The current NiFi user
     * @return Result group
     */
    protected SearchResultGroupDTO buildResultGroup(final ProcessGroup group, final NiFiUser user) {
        if (group == null) {
            return null;
        }

        final SearchResultGroupDTO resultGroup = new SearchResultGroupDTO();
        resultGroup.setId(group.getIdentifier());

        // keep the group name confidential
        if (group.isAuthorized(authorizer, RequestAction.READ, user)) {
            resultGroup.setName(group.getName());
        }

        return resultGroup;
    }
}
