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
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;

public class ProcessGroupSearchResultEnricher extends AbstractComponentSearchResultEnricher {

    public ProcessGroupSearchResultEnricher(final ProcessGroup processGroup, final NiFiUser user, final Authorizer authorizer) {
        super(processGroup, user, authorizer);
    }

    @Override
    public ComponentSearchResultDTO enrich(final ComponentSearchResultDTO input) {
        if (processGroup.getParent() != null) {
            input.setGroupId(processGroup.getParent().getIdentifier());
            input.setParentGroup(buildResultGroup(processGroup.getParent(), user));
            input.setVersionedGroup(buildVersionedGroup(processGroup.getParent(), user));
        } else {
            input.setGroupId(processGroup.getIdentifier());
        }
        return input;
    }
}
