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
package org.apache.nifi.web.search.query;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.groups.ProcessGroup;

import java.util.HashMap;
import java.util.Map;

public class MapBasedSearchQuery implements SearchQuery {
    private final String term;
    private final Map<String, String> filters = new HashMap<>();
    private final NiFiUser user;
    private final ProcessGroup rootGroup;
    private final ProcessGroup activeGroup;

    public MapBasedSearchQuery(final String term, final Map<String, String> filters, final NiFiUser user, final ProcessGroup rootGroup, final ProcessGroup activeGroup) {
        this.term = term;
        this.filters.putAll(filters);
        this.user = user;
        this.rootGroup = rootGroup;
        this.activeGroup = activeGroup;

    }

    public String getTerm() {
        return term;
    }

    @Override
    public boolean hasFilter(final String filterName) {
        return filters.containsKey(filterName);
    }

    @Override
    public String getFilter(final String filterName) {
        return filters.get(filterName);
    }

    @Override
    public NiFiUser getUser() {
        return user;
    }

    @Override
    public ProcessGroup getRootGroup() {
        return rootGroup;
    }

    @Override
    public ProcessGroup getActiveGroup() {
        return activeGroup;
    }
}
