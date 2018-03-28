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
package org.apache.nifi.minifi.toolkit.configuration.registry;

import org.apache.nifi.minifi.commons.schema.FlowControllerSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class NiFiRegFlowControllerSchemaFunction implements Function<VersionedFlowSnapshot, FlowControllerSchema> {

    @Override
    public FlowControllerSchema apply(final VersionedFlowSnapshot versionedFlowSnapshot) {

        // If the VersionedFlowSnapshot came directly from NiFi Registry without modification, as would be the
        // case with C2 server, then we should have a non-null VersionedFlow, but if we're using a snapshot that
        // was export from another tool like the CLI, the flow may be null'd out, so fall back to root group.

        String name;
        String description;
        if (versionedFlowSnapshot.getFlow() == null) {
            name = versionedFlowSnapshot.getFlowContents().getName();
            description = versionedFlowSnapshot.getFlowContents().getComments();
        } else {
            name = versionedFlowSnapshot.getFlow().getName();
            description = versionedFlowSnapshot.getFlow().getDescription();
        }

        Map<String, Object> map = new HashMap<>();
        map.put(CommonPropertyKeys.NAME_KEY, name);
        map.put(CommonPropertyKeys.COMMENT_KEY, description);
        return new FlowControllerSchema(map);
    }
}
