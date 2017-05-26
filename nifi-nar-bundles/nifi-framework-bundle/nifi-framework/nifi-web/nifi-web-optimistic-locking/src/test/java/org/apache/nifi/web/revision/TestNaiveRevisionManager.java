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

package org.apache.nifi.web.revision;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.StandardNiFiUser.Builder;
import org.apache.nifi.web.FlowModification;
import org.apache.nifi.web.Revision;

import java.util.HashSet;
import java.util.Set;


public class TestNaiveRevisionManager {
    private static final String CLIENT_1 = "client-1";
    private static final String COMPONENT_1 = "component-1";
    private static final NiFiUser USER_1 = new Builder().identity("user-1").build();

    private RevisionUpdate<Object> components(final Revision revision) {
        return new StandardRevisionUpdate<Object>(null, new FlowModification(revision, null));
    }

    private RevisionUpdate<Object> components(final Revision revision, final Revision... additionalRevisions) {
        final Set<Revision> revisionSet = new HashSet<>();
        for (final Revision rev : additionalRevisions) {
            revisionSet.add(rev);
        }
        return components(revision, revisionSet);
    }

    private RevisionUpdate<Object> components(final Revision revision, final Set<Revision> additionalRevisions) {
        final Set<RevisionUpdate<Object>> components = new HashSet<>();
        for (final Revision rev : additionalRevisions) {
            components.add(new StandardRevisionUpdate<Object>(null, new FlowModification(rev, null)));
        }
        return new StandardRevisionUpdate<Object>(null, new FlowModification(revision, null), additionalRevisions);
    }

}