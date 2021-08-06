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
package org.apache.nifi.registry.revision.standard;

import org.apache.nifi.registry.revision.api.Revision;

import java.util.Comparator;
import java.util.Objects;

public class RevisionComparator implements Comparator<Revision> {

    @Override
    public int compare(final Revision o1, final Revision o2) {
        final int entityComparison = o1.getEntityId().compareTo(o2.getEntityId());
        if (entityComparison != 0) {
            return entityComparison;
        }

        final Comparator<String> nullSafeStringComparator = Comparator.nullsFirst(String::compareTo);
        final int clientComparison = Objects.compare(o1.getClientId(), o2.getClientId(), nullSafeStringComparator);
        if (clientComparison != 0) {
            return clientComparison;
        }

        return o1.getVersion().compareTo(o2.getVersion());
    }

}
