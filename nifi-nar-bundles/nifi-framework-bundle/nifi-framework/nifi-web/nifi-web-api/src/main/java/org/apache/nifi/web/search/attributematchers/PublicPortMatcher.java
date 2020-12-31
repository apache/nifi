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
package org.apache.nifi.web.search.attributematchers;

import org.apache.nifi.connectable.Port;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.List;

import static org.apache.nifi.web.search.attributematchers.AttributeMatcher.addIfMatching;

public class PublicPortMatcher implements AttributeMatcher<Port> {
    private static final String LABEL_USER = "User access control";
    private static final String LABEL_GROUP = "Group access control";

    @Override
    public void match(final Port component, final SearchQuery query, final List<String> matches) {
        final String searchTerm = query.getTerm();

        if (component instanceof PublicPort) {
            final PublicPort publicPort = (PublicPort) component;

            publicPort.getUserAccessControl().forEach(control -> addIfMatching(searchTerm, control, LABEL_USER, matches));
            publicPort.getGroupAccessControl().forEach(control -> addIfMatching(searchTerm, control, LABEL_GROUP, matches));
        }
    }
}
